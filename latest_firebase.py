import os
import time
import secrets
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

import firebase_admin
from firebase_admin import credentials, tenant_mgt
import requests
from supabase import create_client
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession

# =========================
# CONFIG
# =========================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

PROJECT_ID = os.environ["PROJECT_ID"]
API_KEY = os.environ["API_KEY"]
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_JSON", "serviceAccountKey.json")

EMAILS_PER_BATCH = 1000
MAX_TENANT_WORKERS = 5
SLEEP_EMPTY_QUEUE = 2
OF_ID = int(os.getenv("OFFER_ID", 15))

WANTED_TENANT_COUNT = int(os.getenv("WANTED_TENANT_COUNT", 20))
TENANT_DISPLAY_PREFIX = "auto-tenant"

# =========================
# FIREBASE + GOOGLE AUTH
# =========================
cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
firebase_admin.initialize_app(cred)

SCOPES = ["https://www.googleapis.com/auth/identitytoolkit"]
sa_creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
authed_session = AuthorizedSession(sa_creds)

# =========================
# SUPABASE
# =========================
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TENANTS_URL = f"https://identitytoolkit.googleapis.com/v2/projects/{PROJECT_ID}/tenants"
queue = Queue()

# =========================
# HELPERS
# =========================
def random_alpha(n=7):
    return "".join(secrets.choice("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(n))


def fetch_emails(batch_size=EMAILS_PER_BATCH):
    """Fetch emails from Supabase and push to queue"""
    try:
        r = supabase.rpc(
            "get_100_emails_and_insert",
            {"p_table": "gmx_tenant_users", "p_offer_id": OF_ID, "p_limit": batch_size}
        ).execute()

        emails = [row["email"] for row in r.data if row.get("email")]
        for email in emails:
            queue.put(email)

        return len(emails)
    except Exception as e:
        print("❌ Error fetching emails:", e)
        return 0


def send_tenant_password_reset(email, tenant_id):
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:sendOobCode?key={API_KEY}"
    payload = {
        "requestType": "PASSWORD_RESET",
        "email": email,
        "tenantId": tenant_id
    }
    r = requests.post(url, json=payload)
    if r.status_code != 200:
        print(f"❌ Reset failed for {email}: {r.text}")


def add_user_and_send_reset(tenant_id, email):
    tenant_client = tenant_mgt.auth_for_tenant(tenant_id)
    try:
        tenant_client.create_user(
            email=email,
            password=random_alpha(14),
            email_verified=False,
            display_name=f"{random_alpha()} {random_alpha()}"
        )
    except Exception:
        pass  # user may already exist

    send_tenant_password_reset(email, tenant_id)


# =========================
# TENANT MANAGEMENT
# =========================
def get_all_tenants():
    tenants = []
    page_token = None

    while True:
        url = f"{TENANTS_URL}?pageSize=100"
        if page_token:
            url += f"&pageToken={page_token}"

        r = authed_session.get(url).json()
        tenants.extend(r.get("tenants", []))
        page_token = r.get("nextPageToken")

        if not page_token:
            break

    return tenants


def ensure_tenant_count(wanted_count):
    tenants = get_all_tenants()
    existing_count = len(tenants)

    print(f"🏢 Existing tenants: {existing_count}")
    print(f"🎯 Wanted tenants:   {wanted_count}")

    if existing_count >= wanted_count:
        print("✅ Tenant count sufficient")
        return tenants

    missing = wanted_count - existing_count
    print(f"➕ Creating {missing} tenants")

    for i in range(missing):
        display_name = f"{TENANT_DISPLAY_PREFIX}-{existing_count + i + 1}"

        payload = {
            "displayName": display_name,
            "emailSignInConfig": {
                "enabled": True,
                "passwordRequired": True
            }
        }

        r = authed_session.post(TENANTS_URL, json=payload)

        if r.status_code not in (200, 201):
            print(f"❌ Failed to create {display_name}: {r.text}")
        else:
            tenant_id = r.json()["name"].split("/")[-1]
            print(f"✅ Created tenant {display_name} ({tenant_id})")

    return get_all_tenants()


# =========================
# TENANT WORKER
# =========================
def tenant_worker(tenant):
    tenant_id = tenant["name"].split("/")[-1]

    while True:
        try:
            email = queue.get(timeout=10)
        except:
            time.sleep(SLEEP_EMPTY_QUEUE)
            continue

        try:
            add_user_and_send_reset(tenant_id, email)
            print(f"✅ {tenant_id} processed {email}")
        except Exception as e:
            print(f"❌ {tenant_id} error {email}: {e}")

        queue.task_done()


# =========================
# MAIN
# =========================
def main():
    tenants = ensure_tenant_count(WANTED_TENANT_COUNT)

    with ThreadPoolExecutor(max_workers=MAX_TENANT_WORKERS) as executor:
        for tenant in tenants:
            executor.submit(tenant_worker, tenant)

        round_number = 1
        while True:
            print(f"\n🔥 Fetching batch {round_number}")
            fetched = fetch_emails(EMAILS_PER_BATCH)

            if fetched == 0:
                print("⏳ No emails fetched, waiting...")
                time.sleep(5)
            else:
                print(f"📨 {fetched} emails queued")

            round_number += 1
            queue.join()


if __name__ == "__main__":
    main()
