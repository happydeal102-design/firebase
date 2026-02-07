import os
import time
import secrets
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed

import firebase_admin
from firebase_admin import credentials, tenant_mgt
import requests
from supabase import create_client
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession, Request

# =========================
# CONFIG
# =========================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

PROJECT_ID = os.environ["PROJECT_ID"]
API_KEY = os.environ["API_KEY"]
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_JSON", "serviceAccountKey.json")

EMAILS_PER_BATCH = 1000        # Number of emails to fetch per batch
MAX_TENANT_WORKERS = 5         # Number of tenant threads
SLEEP_EMPTY_QUEUE = 2          # Wait before retrying if queue empty
OF_ID = int(os.getenv("OFFER_ID", 15))

# =========================
# FIREBASE + GOOGLE AUTH
# =========================
cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
firebase_admin.initialize_app(cred)

SCOPES = ["https://www.googleapis.com/auth/identitytoolkit"]  # Correct scope
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
lock = threading.Lock()  # For thread-safe prints if needed

# =========================
# HELPERS
# =========================
def random_alpha(n=7):
    return "".join(secrets.choice("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(n))

def fetch_emails(batch_size=EMAILS_PER_BATCH):
    """Fetch emails from Supabase and push to queue"""
    try:
        r = supabase.rpc(
            "get_100_emails_and_insert",  # Modify your RPC to accept batch_size
            {"p_table": "gmx_tenant_users", "p_offer_id": OF_ID, "p_limit": batch_size}
        ).execute()

        emails = [row["email"] for row in r.data if row["email"]]
        for email in emails:
            queue.put(email)

        return len(emails)
    except Exception as e:
        print("Error fetching emails:", e)
        return 0

def send_tenant_password_reset(email, tenant_id):
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:sendOobCode?key={API_KEY}"
    payload = {"requestType": "PASSWORD_RESET", "email": email, "tenantId": tenant_id}
    r = requests.post(url, json=payload)
    if r.status_code != 200:
        print(f"❌ Failed to send email to {email}: {r.text}")

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
            print(f"❌ {tenant_id} error with {email}: {e}")

        queue.task_done()

# =========================
# MAIN LOOP
# =========================
def main():
    tenants = get_all_tenants()

    # Start tenant workers
    with ThreadPoolExecutor(max_workers=MAX_TENANT_WORKERS) as executor:
        for t in tenants:
            executor.submit(tenant_worker, t)

        round_number = 1
        while True:
            print(f"\n🔥 Fetching batch {round_number}")
            fetched = fetch_emails(batch_size=EMAILS_PER_BATCH)
            if fetched == 0:
                print("No emails fetched, waiting 5s")
                time.sleep(5)
            else:
                print(f"Fetched {fetched} emails into queue")
            round_number += 1
            queue.join()  # wait until current batch is fully processed

if __name__ == "__main__":
    main()
