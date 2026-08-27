import hashlib
import json
import time
from datetime import datetime, timedelta, timezone

import pyodbc
import requests

# Testing new branch

MODULE_NAME = "At_REPORT"
TABLE_NAME = "AccountTransactions"

# How far back to re-pull past the last watermark, to catch late/backdated
# postings that show up after the fact with an earlier TransactionDate.
OVERLAP_DAYS = 14

# First-run fallback if there's no watermark yet for a company.
DEFAULT_START_DATE = "2020-04-01"

PER_PAGE = 200
RATE_LIMIT_SLEEP = 0.5

# Sleep between companies before requesting a fresh access token, so we
# don't hammer the Zoho token endpoint in a tight loop across companies.
INTER_COMPANY_SLEEP = 2

# Retry/backoff config specifically for "too many requests" on the token endpoint.
TOKEN_RETRY_ATTEMPTS = 4
TOKEN_RETRY_BASE_WAIT = 5  # seconds; multiplied by attempt number

GET_COMPANIES_SQL = """
SELECT *
FROM dbo.Zoho_Companies_ap
WHERE IsActive = 1;
"""

# ---------------- SQL CONNECTION ---------------- #

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=zoho_incremental_All;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)

print("Connected Successfully!")
cursor = conn.cursor()


# ---------------- HELPERS ---------------- #

def parse_zoho_date(value):
    """Report dates come back as 'YYYY-MM-DD'. Return a date object or None."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        try:
            return datetime.fromisoformat(value).date()
        except Exception:
            return None


def to_plain_date(value):
    """
    Normalize any date-like value (datetime.date, datetime.datetime, or
    None) down to a plain datetime.date. This is the fix for the
    date/datetime comparison bug: SQL Server DATETIME columns come back
    from pyodbc as datetime.datetime, but everything else in this script
    (parse_zoho_date, today) works in plain datetime.date. Mixing the two
    in a > comparison raises TypeError, so every date-like value that
    might be compared gets funneled through here first.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    return value  # already a plain date


def safe_numeric(value):
    """
    Coerce a Zoho report amount field to a float or None.
    Report rows frequently send empty string '' instead of null/0 for
    blank debit/credit amounts, and occasionally comma-formatted
    strings (e.g. '1,234.50') or currency-prefixed strings. SQL
    Server's numeric/decimal columns reject '' outright (Error 8114:
    nvarchar -> numeric), so every amount must be normalized here
    before it reaches a query parameter.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text == "":
        return None
    text = text.replace(",", "")
    text = text.lstrip("$₹€£").strip()
    try:
        return float(text)
    except ValueError:
        return None


def build_transaction_key(org_id, row):
    """
    Stable composite key for a report line, since there's no single
    unique id we can rely on across every org/report variant. Hash the
    fields that together identify one real transaction line. If Zoho
    does return a 'transaction_id' for a row, it's included and does
    almost all the work; the rest guards against collisions when it's
    absent or shared across multiple lines (e.g. split lines).
    """
    parts = [
        str(org_id),
        str(row.get("transaction_id", "")),
        str(row.get("date", "")),
        str(row.get("account_id", row.get("account_name", ""))),
        str(row.get("debit_amount", row.get("debit", ""))),
        str(row.get("credit_amount", row.get("credit", ""))),
        str(row.get("reference_number", "")),
        str(row.get("entity_id", "")),
        str(row.get("description", "")),
    ]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def unwrap_report_rows(page_rows):
    """
    Same unwrap logic as the single-file report script: each page item
    can itself contain a nested 'account_transactions' list (grouped by
    account). Flatten so we get one dict per transaction line, with the
    parent's scalar fields (e.g. account_name) copied onto each line.
    """
    unwrapped = []
    for item in page_rows:
        if isinstance(item, dict) and isinstance(item.get("account_transactions"), list):
            parent_fields = {
                k: v for k, v in item.items()
                if k != "account_transactions" and not isinstance(v, (list, dict))
            }
            for line in item["account_transactions"]:
                if isinstance(line, dict):
                    unwrapped.append({**parent_fields, **line})
        elif isinstance(item, dict):
            unwrapped.append(item)
    return unwrapped


# ---------------- WATERMARK / LOG HELPERS ---------------- #

def get_watermark(company_name):
    row = cursor.execute("""
        SELECT LAST_MODIFIED_TIME
        FROM dbo.ETL_WATERMARK
        WHERE COMPANY_NAME = ?
          AND MODULE_NAME = ?
          AND TABLE_NAME = ?
    """, company_name, MODULE_NAME, TABLE_NAME).fetchone()
    return row[0] if row else None


def update_watermark(company_name, new_watermark):
    cursor.execute("""
        MERGE dbo.ETL_WATERMARK AS T
        USING (SELECT ? AS COMPANY_NAME, ? AS MODULE_NAME, ? AS TABLE_NAME) AS S
        ON T.COMPANY_NAME = S.COMPANY_NAME
        AND T.MODULE_NAME = S.MODULE_NAME
        AND T.TABLE_NAME = S.TABLE_NAME
        WHEN MATCHED THEN
            UPDATE SET LAST_MODIFIED_TIME = ?
        WHEN NOT MATCHED THEN
            INSERT (COMPANY_NAME, MODULE_NAME, TABLE_NAME, LAST_MODIFIED_TIME)
            VALUES (?, ?, ?, ?);
    """,
        company_name, MODULE_NAME, TABLE_NAME,
        new_watermark,
        company_name, MODULE_NAME, TABLE_NAME, new_watermark,
    )
    conn.commit()


def log_run(company_name, status, total_fetched, total_inserted,
            total_updated, total_deleted, error_message=None):
    cursor.execute("""
        INSERT INTO dbo.ETL_REFRESH_LOG
            (COMPANY_NAME, MODULE_NAME, TABLE_NAME, LAST_REFRESH_DATE,
             STATUS, TOTAL_FETCHED, TOTAL_INSERTED, TOTAL_UPDATED,
             TOTAL_DELETED, ERROR_MESSAGE)
        VALUES (?, ?, ?, GETDATE(), ?, ?, ?, ?, ?, ?)
    """, company_name, MODULE_NAME, TABLE_NAME, status,
         total_fetched, total_inserted, total_updated, total_deleted, error_message)
    conn.commit()


def upsert_transaction(company_id, org_id, txn_key, row):
    """
    MERGE one transaction line on the computed TransactionKey.
    Returns 'INSERT' or 'UPDATE' via OUTPUT.
    """
    txn_date = parse_zoho_date(row.get("date"))
    debit_amount = safe_numeric(row.get("debit_amount", row.get("debit")))
    credit_amount = safe_numeric(row.get("credit_amount", row.get("credit")))
    raw_json = json.dumps(row, default=str)

    result = cursor.execute("""
        MERGE dbo.Raw_AccountTransactions AS T
        USING (SELECT ? AS CompanyID, ? AS TransactionKey) AS S
        ON T.CompanyID = S.CompanyID
        AND T.TransactionKey = S.TransactionKey
        WHEN MATCHED THEN
            UPDATE SET
                TransactionDate = ?,
                AccountName     = ?,
                TransactionType = ?,
                ReferenceNumber = ?,
                DebitAmount     = ?,
                CreditAmount    = ?,
                RawJSON         = ?,
                ETLLoadTime     = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (CompanyID, OrganizationID, TransactionKey, TransactionDate,
                    AccountName, TransactionType, ReferenceNumber,
                    DebitAmount, CreditAmount, RawJSON, ETLLoadTime)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
        OUTPUT $action;
    """,
        company_id, txn_key,
        txn_date, row.get("account_name"), row.get("transaction_type"),
        row.get("reference_number"), debit_amount,
        credit_amount, raw_json,
        company_id, org_id, txn_key, txn_date,
        row.get("account_name"), row.get("transaction_type"),
        row.get("reference_number"), debit_amount,
        credit_amount, raw_json,
    ).fetchone()
    return result[0] if result else None


def delete_missing_in_window(company_id, from_date, to_date, source_keys):
    """
    Delete-detection scoped ONLY to the fetched window. We never delete
    rows outside [from_date, to_date] since we have no information about
    them on this run.
    """
    existing_rows = cursor.execute("""
        SELECT TransactionKey FROM dbo.Raw_AccountTransactions
        WHERE CompanyID = ? AND TransactionDate BETWEEN ? AND ?
    """, company_id, from_date, to_date).fetchall()
    existing_keys = {row[0] for row in existing_rows}

    keys_to_delete = existing_keys - source_keys
    for key in keys_to_delete:
        cursor.execute("""
            DELETE FROM dbo.Raw_AccountTransactions
            WHERE CompanyID = ? AND TransactionKey = ?
        """, company_id, key)

    return len(keys_to_delete)


def fetch_report_page(api_url, org_id, headers, from_date, to_date, page):
    url = f"{api_url}/reports/accounttransaction"
    params = {
        "organization_id": org_id,
        "from_date": from_date,
        "to_date": to_date,
        "filter_by": "TransactionDate.CustomDate",
        "response_option": 1,
        "page": page,
        "per_page": PER_PAGE,
    }
    if RATE_LIMIT_SLEEP:
        time.sleep(RATE_LIMIT_SLEEP)
    resp = requests.get(url, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def get_access_token(token_url, client_id, client_secret, refresh_token):
    """
    Request a Zoho access token, with retry + backoff specifically for
    the "You have made too many requests continuously" rejection from
    the token endpoint. Any other failure (bad refresh token, revoked
    client, etc.) raises immediately - only the rate-limit case retries.
    """
    last_token_response = None
    for attempt in range(1, TOKEN_RETRY_ATTEMPTS + 1):
        resp = requests.post(
            token_url,
            data={
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "refresh_token",
            },
            timeout=30,
        )
        token = resp.json()
        last_token_response = token

        if "access_token" in token:
            return token["access_token"]

        error_desc = str(token.get("error_description", "")).lower()
        if "too many requests" in error_desc and attempt < TOKEN_RETRY_ATTEMPTS:
            wait = TOKEN_RETRY_BASE_WAIT * attempt
            print(f"    Token endpoint rate-limited (attempt {attempt}/{TOKEN_RETRY_ATTEMPTS}), "
                  f"waiting {wait}s before retry...")
            time.sleep(wait)
            continue

        # Non-rate-limit error, or retries exhausted: fail now.
        break

    raise RuntimeError(f"Failed to get access token: {last_token_response}")


# ---------------- GET COMPANIES ---------------- #

companies = cursor.execute(GET_COMPANIES_SQL).fetchall()

# ---------------- LOOP THROUGH COMPANIES ---------------- #

for company in companies:

    company_id = company.CompanyID
    company_name = company.CompanyName
    org_id = company.OrganizationID
    client_id = company.ClientID
    client_secret = company.ClientSecret
    refresh_token = company.RefreshToken
    dc = company.DataCenter.lower()

    print(f"Processing {company_name}")

    total_fetched = 0
    total_inserted = 0
    total_updated = 0
    total_deleted = 0
    source_keys = set()

    try:
        # Throttle token requests across companies so we don't fire them
        # back-to-back into Zoho's token endpoint.
        if INTER_COMPANY_SLEEP:
            time.sleep(INTER_COMPANY_SLEEP)

        # ---------------- ACCESS TOKEN ---------------- #

        if dc == "com":
            token_url = "https://accounts.zoho.com/oauth/v2/token"
            api_url = "https://www.zohoapis.com/books/v3"
        else:
            token_url = "https://accounts.zoho.in/oauth/v2/token"
            api_url = "https://www.zohoapis.in/books/v3"

        access_token = get_access_token(token_url, client_id, client_secret, refresh_token)
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

        # ---------------- DATE WINDOW (watermark on TransactionDate) ---------------- #

        # FIX: normalize to a plain date immediately. get_watermark() returns
        # whatever pyodbc gives back for LAST_MODIFIED_TIME (a DATETIME
        # column -> datetime.datetime), but everything downstream
        # (parse_zoho_date, today, max_date_this_run comparisons) needs to
        # be a plain datetime.date or the `>` comparison blows up.
        # ---------------- DATE WINDOW (FULL LOAD) ---------------- #

        today = datetime.now(timezone.utc).date()

        # Always load from the default start date
        from_date = datetime.strptime(DEFAULT_START_DATE, "%Y-%m-%d").date()

        print(f"  Full load: pulling data from {from_date} to {today}")

        from_date_str = from_date.strftime("%Y-%m-%d")
        to_date_str = today.strftime("%Y-%m-%d")

        # No watermark for full load
        watermark_date = None

        # ---------------- PAGINATE THE REPORT ---------------- #

        page = 1
        max_date_this_run = None  # already a plain date now

        while True:
            payload = fetch_report_page(api_url, org_id, headers, from_date_str, to_date_str, page)
            page_rows = payload.get("account_transactions") or payload.get("data") or []

            if not page_rows:
                break

            unwrapped = unwrap_report_rows(page_rows)
            print(f"  Page {page}: {len(page_rows)} raw -> {len(unwrapped)} lines")

            for row in unwrapped:
                txn_key = build_transaction_key(org_id, row)
                source_keys.add(txn_key)

                action = upsert_transaction(company_id, org_id, txn_key, row)
                total_fetched += 1
                if action == "INSERT":
                    total_inserted += 1
                elif action == "UPDATE":
                    total_updated += 1

                row_date = parse_zoho_date(row.get("date"))
                if row_date and (max_date_this_run is None or row_date > max_date_this_run):
                    max_date_this_run = row_date

            conn.commit()

            page_context = payload.get("page_context", {})
            has_more = page_context.get("has_more_page", len(page_rows) >= PER_PAGE)
            if not has_more:
                break
            page += 1

        # ---------------- DELETE MISSING WITHIN THE FETCHED WINDOW ---------------- #

        total_deleted = delete_missing_in_window(company_id, from_date, today, source_keys)
        conn.commit()

        # ---------------- UPDATE WATERMARK + LOG SUCCESS ---------------- #

        if max_date_this_run and max_date_this_run != watermark_date:
            update_watermark(company_name, max_date_this_run)

        log_run(company_name, "SUCCESS", total_fetched, total_inserted, total_updated, total_deleted)
        print(f"  {company_name}: fetched={total_fetched} inserted={total_inserted} "
              f"updated={total_updated} deleted={total_deleted}")

    except Exception as e:
        print(f"  ERROR processing {company_name}: {e}")
        log_run(company_name, "FAILED", total_fetched, total_inserted, total_updated, total_deleted, str(e))
        continue

print("Account Transaction Report ETL Completed")

cursor.close()
conn.close()
# Version 2 test change - Git tracking test
# VERSION 3 - TESTING ROLLBACK
print("THIS IS A BROKEN VERSION")
# PRACTICE CHANGE - Git to GitHub
print("GitHub practice change")
# CI/CD pipeline test - 27 August 2026
