import os
import time
import secrets
import string
import requests
import random
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

import firebase_admin
from firebase_admin import tenant_mgt
from supabase import create_client

from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession, Request

# =============================================================================
# CONFIG FROM ENV
# =============================================================================

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

PROJECT_ID = os.environ["PROJECT_ID"]
API_KEY = os.environ["API_KEY"]

SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "serviceAccountKey.json")

OF_ID = int(os.environ.get("OF_ID", 15))
PAGE_SIZE = 100
RETRY_DELAY = 2

MAX_WORKERS = int(os.environ.get("MAX_WORKERS", 5))

TENANT_API_BASE = f"https://identitytoolkit.googleapis.com/v2/projects/{PROJECT_ID}/tenants"

# =============================================================================
# INIT CLIENTS
# =============================================================================

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
authed_session = AuthorizedSession(credentials)

# 🔥 REQUIRED Firebase Admin initialization
from firebase_admin import credentials as fb_credentials

if not firebase_admin._apps:
    firebase_admin.initialize_app(
        fb_credentials.Certificate(SERVICE_ACCOUNT_FILE),
        {
            "projectId": PROJECT_ID
        }
    )
# =============================================================================
# HELPERS
# =============================================================================

def random_alpha(length: int = 7) -> str:
    return ''.join(secrets.choice(string.ascii_letters) for _ in range(length))

def refresh_access_token() -> str:
    credentials.refresh(Request())
    return credentials.token

# =============================================================================
# SUPABASE
# =============================================================================

def get_email() -> str:
    """Fetch one email from Supabase (retry until success)."""
    while True:
        try:
            res = supabase.rpc(
                "get_one_email_and_insert",
                {"p_table": "gmx_tenant_users", "p_offer_id": OF_ID}
            ).execute()
            return res.data[0]["email"]
        except Exception as e:
            print("Supabase retry:", e)
            time.sleep(RETRY_DELAY)

def ensure_project_row() -> int:
    res = supabase.table("fb_projects").select("*").eq("project_id", PROJECT_ID).execute()
    if res.data:
        return res.data[0]["id"]

    insert = supabase.table("fb_projects").insert({
        "name": PROJECT_ID,
        "project_id": PROJECT_ID
    }).execute()

    return insert.data[0]["id"]

# =============================================================================
# TENANTS
# =============================================================================

def get_project_tenants() -> List[dict]:
    tenants = []
    page_token = None
    while True:
        url = f"{TENANT_API_BASE}?pageSize={PAGE_SIZE}"
        if page_token:
            url += f"&pageToken={page_token}"

        res = authed_session.get(url).json()
        tenants.extend(res.get("tenants", []))
        page_token = res.get("nextPageToken")
        if not page_token:
            break
    return tenants

def create_tenant(display_name: str) -> str:
    payload = {
        "displayName": display_name,
        "allowPasswordSignup": True,
        "enableEmailLinkSignin": False
    }
    res = authed_session.post(TENANT_API_BASE, json=payload).json()
    return res["name"].split("/")[-1]

def update_tenant_inheritance(tenant_id: str):
    token = refresh_access_token()
    url = f"{TENANT_API_BASE}/{tenant_id}"
    params = {"updateMask": "inheritance.emailSendingConfig"}
    payload = {"inheritance": {"emailSendingConfig": True}}

    res = requests.patch(
        url, params=params, json=payload,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    )

    if res.status_code != 200:
        print(f"[FAIL] Inheritance update {tenant_id}: {res.text}")

# =============================================================================
# USERS
# =============================================================================

def send_password_reset(email: str, tenant_id: str):
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:sendOobCode?key={API_KEY}"
    payload = {"requestType": "PASSWORD_RESET", "email": email, "tenantId": tenant_id}
    res = requests.post(url, json=payload)
    if res.status_code != 200:
        print(f"[FAIL] Reset email {email}: {res.text}")

def add_user_and_send_reset(tenant_id: str):
    email = get_email()
    password = random_alpha(7) + random_alpha(7)
    tenant_client = tenant_mgt.auth_for_tenant(tenant_id)
    try:
        tenant_client.create_user(
            email=email,
            password=password,
            display_name=f"{random_alpha()} {random_alpha()}",
            email_verified=False
        )
    except Exception as e:
        print(f"[WARN] User create failed ({email}): {e}")
    send_password_reset(email, tenant_id)
    time.sleep(random.uniform(0.2, 0.6))  # jitter

# =============================================================================
# MULTI-THREADING
# =============================================================================

def process_tenant(tenant: dict):
    tenant_id = tenant["name"].split("/")[-1]
    try:
        add_user_and_send_reset(tenant_id)
        return tenant_id, "ok"
    except Exception as e:
        return tenant_id, f"error: {e}"

# =============================================================================
# MAIN
# =============================================================================

def main():
    supa_project_id = ensure_project_row()
    tenants = get_project_tenants()
    print("Existing tenants:", len(tenants))

    target = int(os.environ.get("TENANTS_TO_CREATE", input("Enter number of tenants for this project: ")))
    to_create = max(0, target - len(tenants))

    for _ in range(to_create):
        tenant_id = create_tenant(random_alpha())
        supabase.table("fb_tenant").insert({"tenant_id": tenant_id, "fb_project_id": supa_project_id}).execute()
        update_tenant_inheritance(tenant_id)
        time.sleep(0.2)

    tenants = get_project_tenants()
    print("Total tenants:", len(tenants))
    print("🚀 Starting multi-threaded user creation...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_tenant, t) for t in tenants]
        for future in as_completed(futures):
            tenant_id, status = future.result()
            print(f"[{tenant_id}] {status}")

    print("✅ Done")

if __name__ == "__main__":
    main()
