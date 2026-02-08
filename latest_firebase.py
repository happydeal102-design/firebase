import os
import time
import secrets
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

EMAILS_PER_BATCH = int(os.getenv("EMAILS_PER_BATCH", 1000))
MAX_TENANT_WORKERS = int(os.getenv("MAX_TENANT_WORKERS", 5))
SLEEP_EMPTY_QUEUE = 2
OF_ID = int(os.getenv("OFFER_ID", 15))

# Fixed tenants
TENANT_IDS_RAW = os.getenv("TENANT_IDS", "")
TENANT_IDS = [t.strip() for t in TENANT_IDS_RAW.split(",") if t.strip()]

if len(TENANT_IDS) != 5:
    raise ValueError("❌ Exactly 5 tenant IDs must be provided")

TENANTS = [{"name": f"projects/{PROJECT_ID}/tenants/{tid}"} for tid in TENANT_IDS]

queue = Queue()

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

# =========================
# HELPERS
# =========================
def random_alpha(n=7):
    return "".join(secrets.choice("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(n))


def fetch_emails(batch_size):
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
        print("❌ Supabase error:", e)
        return 0


def send_tenant_password_reset(email, tenant_id):
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:sendOobCode?key={API_KEY}"
    payload = {"requestType": "PASSWORD_RESET", "email": email, "tenantId": tenant_id}
    r = requests.post(url, json=payload)
    if r.status_code != 200:
        print(f"❌ Reset failed {email}: {r.text}")


def add_user_and_send_reset(tenant_id, email):
    client = tenant_mgt.auth_for_tenant(tenant_id)
    try:
        client.create_user(
            email=email,
            password=random_alpha(14),
            email_verified=False,
            display_name=f"{random_alpha()} {random_alpha()}"
        )
    except Exception:
        pass
    send_tenant_password_reset(email, tenant_id)


# =========================
# WORKER
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
            print(f"✅ {tenant_id} → {email}")
        except Exception as e:
            print(f"❌ {tenant_id} → {email}: {e}")

        queue.task_done()


# =========================
# MAIN
# =========================
def main():
    print(f"🎯 Using tenants: {TENANT_IDS}")

    with ThreadPoolExecutor(max_workers=len(TENANTS)) as executor:
        for tenant in TENANTS:
            executor.submit(tenant_worker, tenant)

        round_num = 1
        while True:
            print(f"\n🔥 Batch {round_num}")
            fetched = fetch_emails(EMAILS_PER_BATCH)
            print(f"📨 Emails fetched: {fetched}")
            round_num += 1
            queue.join()


if __name__ == "__main__":
    main()
