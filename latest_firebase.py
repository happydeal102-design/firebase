import os
import sys
import time
import secrets
import string
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import firebase_admin
from firebase_admin import credentials, tenant_mgt
import requests
from supabase import create_client
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession, Request

# =========================
# CONFIG (ENV / SECRETS)
# =========================

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

PROJECT_ID = os.environ["PROJECT_ID"]
API_KEY = os.environ["API_KEY"]
#SERVICE_ACCOUNT_JSON = os.environ["SERVICE_ACCOUNT_JSON"]
SERVICE_ACCOUNT_FILE = os.environ.get(
    "SERVICE_ACCOUNT_JSON",
    "serviceAccountKey.json"
)

KILL_SWITCH = os.getenv("KILL_SWITCH", "0")

EMAILS_PER_TENANT = int(os.getenv("EMAILS_PER_TENANT", "3000"))
MAX_TENANT_WORKERS = int(os.getenv("MAX_TENANT_WORKERS", "5"))
SLEEP_BETWEEN_ROUNDS = int(os.getenv("SLEEP_BETWEEN_ROUNDS", "10"))

OF_ID = int(os.getenv("OFFER_ID", "15"))

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

# =========================
# INIT
# =========================

cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
firebase_admin.initialize_app(cred)

sa_creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_JSON, scopes=SCOPES
)
authed_session = AuthorizedSession(sa_creds)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TENANTS_URL = f"https://identitytoolkit.googleapis.com/v2/projects/{PROJECT_ID}/tenants"

lock = threading.Lock()

# =========================
# HELPERS
# =========================

def random_alpha(n=7):
    return "".join(secrets.choice(string.ascii_letters) for _ in range(n))


def get_email_from_supabase():
    try:
        r = supabase.rpc(
            "get_one_email_and_insert",
            {"p_table": "gmx_tenant_users", "p_offer_id": OF_ID}
        ).execute()

        return r.data[0]["email"] if r.data and r.data[0]["email"] else None
    except Exception as e:
        print("Supabase error:", e)
        return None


def send_tenant_password_reset(email, tenant_id):
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:sendOobCode?key={API_KEY}"
    payload = {
        "requestType": "PASSWORD_RESET",
        "email": email,
        "tenantId": tenant_id
    }

    r = requests.post(url, json=payload)
    if r.status_code != 200:
        raise Exception(r.text)


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
# TENANT WORKER
# =========================

def process_tenant_batch(tenant: dict):
    tenant_id = tenant["name"].split("/")[-1]
    sent = 0

    print(f"🚀 Tenant {tenant_id} started")

    while sent < EMAILS_PER_TENANT:
        if KILL_SWITCH == "1":
            print("🛑 Kill switch enabled")
            sys.exit(0)

        email = get_email_from_supabase()
        if not email:
            time.sleep(2)
            continue

        try:
            add_user_and_send_reset(tenant_id, email)
            sent += 1

            if sent % 100 == 0:
                print(f"📧 {tenant_id}: {sent}/{EMAILS_PER_TENANT}")

            time.sleep(0.05)  # rate safety

        except Exception as e:
            print(f"❌ {tenant_id}: {e}")
            time.sleep(1)

    print(f"✅ Tenant {tenant_id} finished batch")


# =========================
# TENANT FETCH
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


# =========================
# RUNNER
# =========================

def run_round(tenants):
    with ThreadPoolExecutor(max_workers=MAX_TENANT_WORKERS) as executor:
        futures = [
            executor.submit(process_tenant_batch, t)
            for t in tenants
        ]

        for f in as_completed(futures):
            f.result()


def infinite_runner():
    round_num = 1

    while True:
        print(f"\n🔥 ROUND {round_num} START")
        tenants = get_all_tenants()

        run_round(tenants)

        round_num += 1
        print(f"😴 Sleeping {SLEEP_BETWEEN_ROUNDS}s")
        time.sleep(SLEEP_BETWEEN_ROUNDS)


# =========================
# ENTRY
# =========================

if __name__ == "__main__":
    infinite_runner()
