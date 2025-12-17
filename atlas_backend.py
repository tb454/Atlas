from __future__ import annotations

import os, json, hashlib, pathlib, secrets
from datetime import datetime, timezone
from typing import Any, Dict

import bcrypt
from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form, Query
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors


# ----------------------------
# Config / DB
# ----------------------------

def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None else default

def get_engine() -> Engine:
    dsn = _env("ATLAS_DATABASE_URL")
    if not dsn:
        raise RuntimeError("ATLAS_DATABASE_URL not set")
    return create_engine(dsn, pool_pre_ping=True)

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def client_ip(request: Request) -> str | None:
    """
    Proxy-safe client IP:
    - Render / reverse proxies set X-Forwarded-For
    - first IP is the original client
    """
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else None


VAULT_DIR = pathlib.Path(_env("ATLAS_VAULT_DIR", "./vault_storage")).resolve()
VAULT_DIR.mkdir(parents=True, exist_ok=True)

DOCS_DIR = (VAULT_DIR / "docs").resolve()
DOCS_DIR.mkdir(parents=True, exist_ok=True)

ADMIN_EMAIL = _env("ATLAS_ADMIN_EMAIL", "admin@atlas.local").lower().strip()
ADMIN_PASSWORD = _env("ATLAS_ADMIN_PASSWORD", "ChangeMeNow123!")


# ----------------------------
# App
# ----------------------------

app = FastAPI(title="Atlas Backend", version="0.1")

app.add_middleware(
    SessionMiddleware,
    secret_key=_env("SESSION_SECRET", secrets.token_urlsafe(32)),
    same_site="lax",
    https_only=False,  # set True behind HTTPS
)

trusted = [h.strip() for h in _env("TRUSTED_HOSTS", "").split(",") if h.strip()]
if trusted:
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=trusted)

# CORS: keep simple for now (same-origin UI doesn’t need credentials)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = pathlib.Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ----------------------------
# DDL bootstrap
# ----------------------------

DDL = """
create extension if not exists pgcrypto;

create table if not exists atlas_users (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  email text not null unique,
  password_hash text not null,
  role text not null default 'admin'
);

create table if not exists ip_owners (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  legal_name text not null,
  entity_type text,
  jurisdiction text,
  address text,
  email text,
  phone text
);

alter table if exists atlas_users
  add column if not exists owner_id uuid;

alter table if exists atlas_users
  add constraint if not exists fk_atlas_users_owner_id
  foreign key (owner_id) references ip_owners(id) on delete set null;

create index if not exists idx_atlas_users_owner_id on atlas_users(owner_id);


create table if not exists ip_assets (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  owner_id uuid references ip_owners(id) on delete set null,
  title text not null,
  asset_type text,
  jurisdictions text,
  reg_no text,
  status text,
  priority_date date,
  inventors text,
  current_owner_entity text,
  encumbrances text,
  description text,
  targets text,
  active boolean not null default true
);

create index if not exists idx_ip_assets_owner on ip_assets(owner_id);
create index if not exists idx_ip_assets_created on ip_assets(created_at desc);

create table if not exists ip_agreements (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  owner_id uuid references ip_owners(id) on delete set null,
  status text not null default 'draft',
  effective_date date,
  fee_percent numeric not null default 20
);

create table if not exists licenses (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  ip_asset_id uuid references ip_assets(id) on delete set null,
  licensee text not null,
  currency text not null default 'USD',
  gross_amount numeric,
  notes text,
  executed_at timestamptz
);

create table if not exists payouts (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  owner_id uuid references ip_owners(id) on delete set null,
  period_start date,
  period_end date,
  currency text not null default 'USD',
  gross_receipts numeric not null default 0,
  taxes numeric not null default 0,
  withholding numeric not null default 0,
  fees numeric not null default 0,
  expenses numeric not null default 0,
  reserve_adj numeric not null default 0,
  net_to_owner numeric not null default 0,
  status text not null default 'pending'
);

create table if not exists atlas_onboarding_submissions (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  status text not null default 'submitted',

  owner_email text,
  owner_name text,  
  entity_type text,
  jurisdiction text,

  owner_json jsonb,

  nda_json jsonb,
  intake_json jsonb,
  attestation_json jsonb,
  participation_json jsonb,
  billing_ack_json jsonb,
  doc_versions_json jsonb,

  ip_assets_count int not null default 0,
  notes text,

  user_agent text,
  ip_address text
);

alter table if exists atlas_onboarding_submissions
  add column if not exists approved_owner_id uuid references ip_owners(id) on delete set null;

alter table if exists atlas_onboarding_submissions
  add column if not exists approved_user_id uuid references atlas_users(id) on delete set null;

alter table if exists atlas_onboarding_submissions
  add column if not exists approved_at timestamptz;

create index if not exists idx_atlas_onboarding_created_at on atlas_onboarding_submissions (created_at desc);
create index if not exists idx_atlas_onboarding_owner_email on atlas_onboarding_submissions (owner_email);

create table if not exists vault_sources (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  source_key text not null unique,
  description text
);

insert into vault_sources (source_key, description)
values ('dossier', 'Dossier exports'), ('bridge', 'BRidge exports')
on conflict (source_key) do nothing;

create table if not exists vault_objects (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),

  source_key text not null references vault_sources(source_key),
  org_id text,
  tenant_id text,
  schema_version text,

  filename text not null,
  content_type text,
  byte_size bigint not null default 0,
  sha256 text not null,

  manifest_json jsonb,
  stored_path text not null
);

create index if not exists idx_vault_objects_created on vault_objects(created_at desc);
create index if not exists idx_vault_objects_source on vault_objects(source_key);

create table if not exists atlas_documents (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  owner_id uuid references ip_owners(id) on delete cascade,
  onboarding_id uuid references atlas_onboarding_submissions(id) on delete set null,

  doc_type text not null, -- nda / attestation / mippa_ack / billing_ack / zip_pack
  doc_version text,
  filename text not null,
  sha256 text not null,
  stored_path text not null
);

create index if not exists idx_atlas_documents_owner on atlas_documents(owner_id);
create index if not exists idx_atlas_documents_created on atlas_documents(created_at desc);

create table if not exists vault_access_logs (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  object_id uuid references vault_objects(id) on delete cascade,
  actor_email text,
  action text,
  ip_address text,
  user_agent text
);
"""

def run_ddl():
    eng = get_engine()
    with eng.begin() as conn:
        for stmt in [s.strip() for s in DDL.split(";")]:
            if stmt:
                conn.execute(text(stmt))

def ensure_admin():
    eng = get_engine()
    pw_hash = bcrypt.hashpw(ADMIN_PASSWORD.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    with eng.begin() as conn:
        r = conn.execute(text("select id from atlas_users where email=:e"), {"e": ADMIN_EMAIL}).fetchone()
        if not r:
            conn.execute(
                text("insert into atlas_users (email, password_hash, role) values (:e, :p, 'admin')"),
                {"e": ADMIN_EMAIL, "p": pw_hash},
            )

@app.on_event("startup")
def _startup():
    run_ddl()
    ensure_admin()


# ----------------------------
# Auth helpers
# ----------------------------

def require_admin(req: Request) -> str:
    email = req.session.get("email")
    role = req.session.get("role")
    if not email or role != "admin":
        raise HTTPException(status_code=401, detail="login required")
    return str(email)

def require_owner(req: Request) -> str:
    email = req.session.get("email")
    role = req.session.get("role")
    owner_id = req.session.get("owner_id")
    if not email or role != "owner" or not owner_id:
        raise HTTPException(status_code=401, detail="owner login required")
    return str(owner_id)


@app.post("/api/auth/login")
async def login(payload: Dict[str, Any], request: Request):
    email = (payload.get("email") or "").lower().strip()
    password = (payload.get("password") or "").strip()
    if not email or not password:
        raise HTTPException(status_code=400, detail="email and password required")

    eng = get_engine()
    with eng.begin() as conn:
        r = conn.execute(
            text("select email, password_hash, role, owner_id from atlas_users where email=:e"),
            {"e": email},
        ).fetchone()

    if not r:
        raise HTTPException(status_code=401, detail="invalid credentials")
    if not bcrypt.checkpw(password.encode("utf-8"), r.password_hash.encode("utf-8")):
        raise HTTPException(status_code=401, detail="invalid credentials")

    request.session["email"] = r.email
    request.session["role"] = r.role
    request.session["owner_id"] = str(r.owner_id) if r.owner_id else None
    return {"ok": True, "email": r.email, "role": r.role, "owner_id": request.session["owner_id"]}

@app.post("/api/auth/logout")
async def logout(request: Request):
    request.session.clear()
    return {"ok": True}

@app.get("/api/auth/me")
async def me(request: Request):
    return {
        "email": request.session.get("email"),
        "role": request.session.get("role"),
        "owner_id": request.session.get("owner_id"),
    }

@app.get("/api/owner/me")
async def owner_me(request: Request):
    owner_id = require_owner(request)
    eng = get_engine()
    with eng.begin() as conn:
        o = conn.execute(text("""
          select id, created_at, legal_name, entity_type, jurisdiction, address, email, phone
          from ip_owners
          where id = :id::uuid
        """), {"id": owner_id}).fetchone()
    if not o:
        raise HTTPException(status_code=404, detail="owner record not found")
    return {"ok": True, "owner": dict(o._mapping)}

@app.get("/api/owner/assets")
async def owner_assets(request: Request, limit: int = Query(200, ge=1, le=500)):
    owner_id = require_owner(request)
    eng = get_engine()
    with eng.begin() as conn:
        rows = conn.execute(text("""
          select id, created_at, title, asset_type, jurisdictions, reg_no, status, priority_date,
                 inventors, current_owner_entity, encumbrances, description, targets, active
          from ip_assets
          where owner_id = :oid::uuid
          order by created_at desc
          limit :limit
        """), {"oid": owner_id, "limit": limit}).fetchall()
    return {"ok": True, "items": [dict(r._mapping) for r in rows]}

@app.get("/api/owner/onboarding")
async def owner_onboarding(request: Request):
    owner_id = require_owner(request)
    eng = get_engine()
    with eng.begin() as conn:
        r = conn.execute(text("""
          select id, created_at, status, notes, owner_email, owner_name,
                 nda_json, intake_json, attestation_json, participation_json, billing_ack_json, doc_versions_json
          from atlas_onboarding_submissions
          where approved_owner_id = :oid::uuid
          order by created_at desc
          limit 1
        """), {"oid": owner_id}).fetchone()
    if not r:
        return {"ok": True, "submission": None}
    return {"ok": True, "submission": dict(r._mapping)}

def _rand_password(n: int = 14) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789!@#$%_-"
    return "".join(secrets.choice(alphabet) for _ in range(n))

@app.post("/api/admin/onboarding/{onboarding_id}/approve")
async def approve_onboarding(onboarding_id: str, request: Request):
    actor = require_admin(request)
    eng = get_engine()

    temp_password = _rand_password()
    pw_hash = bcrypt.hashpw(temp_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    with eng.begin() as conn:
        sub = conn.execute(text("""
          select id, status, owner_email, owner_name, entity_type, jurisdiction,
            owner_json, intake_json, doc_versions_json,
            nda_json, attestation_json, participation_json, billing_ack_json
          from atlas_onboarding_submissions
          where id = :id::uuid
        """), {"id": onboarding_id}).fetchone()

        if not sub:
            raise HTTPException(status_code=404, detail="Not found")

        if sub.status not in ("submitted", "needs_more"):
            raise HTTPException(status_code=400, detail=f"Cannot approve from status={sub.status}")

        owner_json = sub.owner_json or {}
        intake_json = sub.intake_json or {}
        doc_versions = sub.doc_versions_json or {}
        nda_json = sub.nda_json or {}
        att_json = sub.attestation_json or {}
        part_json = sub.participation_json or {}
        bill_json = sub.billing_ack_json or {}
        ip_assets = (intake_json.get("ip_assets") or [])

        # Create ip_owners
        owner_row = conn.execute(text("""
          insert into ip_owners (legal_name, entity_type, jurisdiction, address, email, phone)
          values (:legal_name, :entity_type, :jurisdiction, :address, :email, :phone)
          returning id
        """), {
            "legal_name": (owner_json.get("legal_name") or sub.owner_name or "").strip(),
            "entity_type": (owner_json.get("entity_type") or sub.entity_type or "").strip(),
            "jurisdiction": (owner_json.get("jurisdiction") or sub.jurisdiction or "").strip(),
            "address": (owner_json.get("address") or "").strip(),
            "email": (owner_json.get("email") or sub.owner_email or "").strip(),
            "phone": (owner_json.get("phone") or "").strip(),
        }).fetchone()
        owner_id = str(owner_row[0])

        # Create owner user login (email must be unique)
        email = (owner_json.get("email") or sub.owner_email or "").lower().strip()
        if not email:
            raise HTTPException(status_code=400, detail="Owner email missing; cannot create login")

        # If a user already exists with that email, fail (keeps you safe)
        existing = conn.execute(text("select id from atlas_users where email=:e"), {"e": email}).fetchone()
        if existing:
            raise HTTPException(status_code=400, detail="User already exists for this email")

        user_row = conn.execute(text("""
          insert into atlas_users (email, password_hash, role, owner_id)
          values (:email, :pw, 'owner', :owner_id::uuid)
          returning id
        """), {"email": email, "pw": pw_hash, "owner_id": owner_id}).fetchone()
        user_id = str(user_row[0])

        # Create ip_assets
        for a in ip_assets:
            title = str(a.get("title","")).strip()
            desc = str(a.get("description","")).strip()
            if not title or not desc:
                continue
            conn.execute(text("""
              insert into ip_assets
                (owner_id, title, asset_type, jurisdictions, reg_no, status, priority_date,
                 inventors, current_owner_entity, encumbrances, description, targets)
              values
                (:owner_id::uuid, :title, :asset_type, :jurisdictions, :reg_no, :status, nullif(:priority_date,'')::date,
                 :inventors, :current_owner_entity, :encumbrances, :description, :targets)
            """), {
                "owner_id": owner_id,
                "title": title,
                "asset_type": str(a.get("asset_type","")).strip(),
                "jurisdictions": str(a.get("jurisdictions","")).strip(),
                "reg_no": str(a.get("reg_no","")).strip(),
                "status": str(a.get("status","")).strip(),
                "priority_date": str(a.get("priority_date","")).strip(),
                "inventors": str(a.get("inventors","")).strip(),
                "current_owner_entity": str(a.get("current_owner_entity","")).strip(),
                "encumbrances": str(a.get("encumbrances","")).strip(),
                "description": desc,
                "targets": str(a.get("targets","")).strip(),
            })

        # Mark submission approved + link records
        conn.execute(text("""
          update atlas_onboarding_submissions
          set status='approved',
              approved_owner_id = :oid::uuid,
              approved_user_id = :uid::uuid,
              approved_at = now()
          where id = :sid::uuid
        """), {"oid": owner_id, "uid": user_id, "sid": onboarding_id})

        # Generate executed PDFs + store in atlas_documents
       
        owner_name_exec = (owner_json.get("legal_name") or sub.owner_name or "").strip()
        owner_email_exec = (owner_json.get("email") or sub.owner_email or "").strip()
        signer_name = (att_json.get("signer_name") or "").strip()
        signer_title = (att_json.get("signer_title") or "").strip()
        att_date = (att_json.get("date") or "").strip()

        # doc versions (from onboarding page hidden fields)
        mippa_ver = str(doc_versions.get("mippa_version") or "Atlas-MIPPA-v1")
        billing_ver = str(doc_versions.get("billing_policy_version") or "Atlas-Billing-Payout-Policy-v1")
        nda_ver = str(doc_versions.get("nda_version") or "Atlas-NDA-v1")

        # 1) NDA executed cert (only if enabled)
        if bool(nda_json.get("enabled")):
            nda_out = DOCS_DIR / f"nda_exec_{onboarding_id}_{_safe_filename(owner_email_exec)}.pdf"
            _write_exec_pdf(
                out_path=nda_out,
                title="Executed NDA Acknowledgment",
                subtitle="Atlas Mutual NDA • executed record",
                fields=[
                    ("Owner / Counterparty", owner_name_exec),
                    ("Email", owner_email_exec),
                    ("Effective Date", str(nda_json.get("effective_date") or "")),
                    ("Counterparty Name", str(nda_json.get("counterparty_name") or "")),
                    ("Counterparty Type", str(nda_json.get("counterparty_type") or "")),
                    ("Signer Name (typed)", str(nda_json.get("signer_name") or "")),
                    ("Signer Title", str(nda_json.get("signer_title") or "")),
                    ("Non-Solicit Included", "YES" if nda_json.get("non_solicit") else "NO"),
                    ("Residuals Included", "YES" if nda_json.get("residuals") else "NO"),
                    ("Doc Version", nda_ver),
                ],
            )
            nda_sha = sha256_file(nda_out)
            _store_document_row(
                conn=conn,
                owner_id=owner_id,
                onboarding_id=onboarding_id,
                doc_type="nda",
                doc_version=nda_ver,
                filename=nda_out.name,
                stored_path=nda_out,
                sha256=nda_sha,
            )

        # 2) Attestation executed cert
        att_out = DOCS_DIR / f"attestation_exec_{onboarding_id}_{_safe_filename(owner_email_exec)}.pdf"
        _write_exec_pdf(
            out_path=att_out,
            title="Executed Owner Attestation",
            subtitle="IP Owner Attestation & Authorization • executed record",
            fields=[
                ("Owner Legal Name", owner_name_exec),
                ("Owner Email", owner_email_exec),
                ("Signer Name (typed)", signer_name),
                ("Signer Title", signer_title),
                ("Attestation Date", att_date),
                ("Confirm Ownership", "YES" if att_json.get("confirm_ownership") else "NO"),
                ("Confirm Accuracy", "YES" if att_json.get("confirm_accuracy") else "NO"),
                ("Ack No Legal/Tax Advice", "YES" if att_json.get("ack_no_legal") else "NO"),
                ("Doc Version", "Atlas-IP-Owner-Attestation-v1"),
            ],
        )
        att_sha = sha256_file(att_out)
        _store_document_row(
            conn=conn,
            owner_id=owner_id,
            onboarding_id=onboarding_id,
            doc_type="attestation",
            doc_version="Atlas-IP-Owner-Attestation-v1",
            filename=att_out.name,
            stored_path=att_out,
            sha256=att_sha,
        )

        # 3) Participation Agreement acceptance cert
        pa_out = DOCS_DIR / f"mippa_ack_{onboarding_id}_{_safe_filename(owner_email_exec)}.pdf"
        _write_exec_pdf(
            out_path=pa_out,
            title="Participation Agreement Acceptance",
            subtitle="Atlas Master IP Participation Agreement • acceptance record",
            fields=[
                ("Owner Legal Name", owner_name_exec),
                ("Owner Email", owner_email_exec),
                ("Agreement Effective Date", str(part_json.get("effective_date") or "")),
                ("Accepted", "YES" if part_json.get("accepted") else "NO"),
                ("Fee", "20% of Gross Receipts (default)"),
                ("Doc Version", mippa_ver),
            ],
        )
        pa_sha = sha256_file(pa_out)
        _store_document_row(
            conn=conn,
            owner_id=owner_id,
            onboarding_id=onboarding_id,
            doc_type="mippa_ack",
            doc_version=mippa_ver,
            filename=pa_out.name,
            stored_path=pa_out,
            sha256=pa_sha,
        )

        # 4) Billing Policy acceptance cert
        bp_out = DOCS_DIR / f"billing_ack_{onboarding_id}_{_safe_filename(owner_email_exec)}.pdf"
        _write_exec_pdf(
            out_path=bp_out,
            title="Billing & Payout Policy Acknowledgment",
            subtitle="Atlas Billing & Payout Policy • acceptance record",
            fields=[
                ("Owner Legal Name", owner_name_exec),
                ("Owner Email", owner_email_exec),
                ("Accepted", "YES" if bill_json.get("accepted") else "NO"),
                ("Doc Version", billing_ver),
            ],
        )
        bp_sha = sha256_file(bp_out)
        _store_document_row(
            conn=conn,
            owner_id=owner_id,
            onboarding_id=onboarding_id,
            doc_type="billing_ack",
            doc_version=billing_ver,
            filename=bp_out.name,
            stored_path=bp_out,
            sha256=bp_sha,
        )


        # audit log
        conn.execute(text("""
          insert into vault_access_logs (object_id, actor_email, action, ip_address, user_agent)
          values (null, :actor, 'approve_onboarding', :ip, :ua)
        """), {
            "actor": actor,
            "ip": client_ip(request),
            "ua": request.headers.get("user-agent",""),
        })

    return {
        "ok": True,
        "onboarding_id": onboarding_id,
        "owner_id": owner_id,
        "user_email": email,
        "temp_password": temp_password
    }

# ----------------------------
# Pages
# ----------------------------

@app.get("/", response_class=RedirectResponse)
async def root():
    return RedirectResponse("/static/atlas-login.html")

@app.get("/admin", response_class=RedirectResponse)
async def admin_redirect():
    return RedirectResponse("/static/atlas-admin.html")

@app.get("/onboard", response_class=RedirectResponse)
async def onboard_redirect():
    return RedirectResponse("/static/atlas-onboard.html")


# ----------------------------
# Onboarding intake (public)
# ----------------------------

@app.post("/api/atlas/onboarding")
async def submit_onboarding(payload: Dict[str, Any], request: Request):
    owner = payload.get("owner") or {}
    owner_email = (owner.get("email") or "").strip()
    owner_name = (owner.get("legal_name") or "").strip()
    entity_type = (owner.get("entity_type") or "").strip()
    jurisdiction = (owner.get("jurisdiction") or "").strip()

    intake = payload.get("intake") or {}
    ip_assets = intake.get("ip_assets") or []
    ip_assets_count = len(ip_assets)

    if not owner_email or not owner_name:
        raise HTTPException(status_code=400, detail="owner.email and owner.legal_name required")
    if ip_assets_count < 1:
        raise HTTPException(status_code=400, detail="At least 1 IP asset is required")

    # server-side validation (don’t trust the browser)
    nda = payload.get("nda") or {}
    att = payload.get("attestation") or {}
    part = payload.get("participation") or {}
    bill = payload.get("billing_ack") or {}

    if nda.get("enabled") and not nda.get("accepted"):
        raise HTTPException(status_code=400, detail="NDA enabled but not accepted")

    if not att.get("signer_name"):
        raise HTTPException(status_code=400, detail="attestation.signer_name is required")
    if not (att.get("confirm_ownership") and att.get("confirm_accuracy") and att.get("ack_no_legal")):
        raise HTTPException(status_code=400, detail="All attestation confirmations are required")

    if not part.get("accepted"):
        raise HTTPException(status_code=400, detail="Participation agreement must be accepted")
    if not bill.get("accepted"):
        raise HTTPException(status_code=400, detail="Billing policy must be acknowledged")

    bad = [
        a for a in ip_assets
        if not (str(a.get("title", "")).strip() and str(a.get("description", "")).strip())
    ]
    if bad:
        raise HTTPException(status_code=400, detail="Each IP asset requires title + description")

    ua = request.headers.get("user-agent", "")
    ip_addr = client_ip(request)

    eng = get_engine()
    with eng.begin() as conn:
        row = conn.execute(text("""
            insert into atlas_onboarding_submissions
              (owner_email, owner_name, entity_type, jurisdiction,
              owner_json,
              nda_json, intake_json, attestation_json, participation_json, billing_ack_json, doc_versions_json,
              ip_assets_count, user_agent, ip_address)
            values
              (:owner_email, :owner_name, :entity_type, :jurisdiction,
              :owner_json::jsonb,
              :nda_json::jsonb, :intake_json::jsonb, :attestation_json::jsonb, :participation_json::jsonb, :billing_ack_json::jsonb, :doc_versions_json::jsonb,
              :ip_assets_count, :user_agent, :ip_address)
            returning id
        """), {
            "owner_email": owner_email,
            "owner_name": owner_name,
            "entity_type": entity_type,
            "jurisdiction": jurisdiction,
            "owner_json": json.dumps(owner),
            "nda_json": json.dumps(nda),
            "intake_json": json.dumps(intake),
            "attestation_json": json.dumps(att),
            "participation_json": json.dumps(part),
            "billing_ack_json": json.dumps(bill),
            "doc_versions_json": json.dumps(payload.get("doc_versions") or {}),
            "ip_assets_count": ip_assets_count,
            "user_agent": ua,
            "ip_address": ip_addr
        }).fetchone()
        onboarding_id = str(row[0])

    return {"ok": True, "onboarding_id": onboarding_id, "status": "submitted"}


# ----------------------------
# Admin: onboarding review
# ----------------------------

@app.get("/api/admin/onboarding")
async def list_onboarding(
    request: Request,
    status: str = Query("submitted"),
    limit: int = Query(50, ge=1, le=200),
):
    actor = require_admin(request)
    eng = get_engine()
    with eng.begin() as conn:
        rows = conn.execute(text("""
            select id, created_at, status, owner_email, owner_name, ip_assets_count
            from atlas_onboarding_submissions
            where (:status = 'all' or status = :status)
            order by created_at desc
            limit :limit
        """), {"status": status, "limit": limit}).fetchall()
    return {"ok": True, "items": [dict(r._mapping) for r in rows]}

@app.get("/api/admin/onboarding/{onboarding_id}")
async def get_onboarding(onboarding_id: str, request: Request):
    actor = require_admin(request)
    eng = get_engine()
    with eng.begin() as conn:
        r = conn.execute(text("""
            select *
            from atlas_onboarding_submissions
            where id = :id
        """), {"id": onboarding_id}).fetchone()

        if not r:
            raise HTTPException(status_code=404, detail="Not found")

        # log read (object_id null is allowed)
        conn.execute(text("""
            insert into vault_access_logs (object_id, actor_email, action, ip_address, user_agent)
            values (null, :actor, 'view_onboarding', :ip, :ua)
        """), {
            "actor": actor,
            "ip": client_ip(request),
            "ua": request.headers.get("user-agent", "")
        })

    return {"ok": True, "submission": dict(r._mapping)}

@app.post("/api/admin/onboarding/{onboarding_id}/status")
async def set_onboarding_status(onboarding_id: str, payload: Dict[str, Any], request: Request):
    actor = require_admin(request)
    status = (payload.get("status") or "").strip()
    notes = (payload.get("notes") or "").strip()
    if status not in {"submitted", "needs_more", "approved", "rejected"}:
        raise HTTPException(status_code=400, detail="invalid status")

    eng = get_engine()
    with eng.begin() as conn:
        n = conn.execute(text("""
            update atlas_onboarding_submissions
            set status = :s, notes = :notes
            where id = :id
        """), {"s": status, "notes": notes, "id": onboarding_id}).rowcount

        if n == 0:
            raise HTTPException(status_code=404, detail="Not found")

        conn.execute(text("""
            insert into vault_access_logs (object_id, actor_email, action, ip_address, user_agent)
            values (null, :actor, 'set_onboarding_status', :ip, :ua)
        """), {
            "actor": actor,
            "ip": client_ip(request),
            "ua": request.headers.get("user-agent", "")
        })

    return {"ok": True, "id": onboarding_id, "status": status}


# ----------------------------
# Vault: ingest + list + download (admin)
# ----------------------------

def _safe_filename(name: str) -> str:
    keep = "._-"
    return "".join(c for c in name if c.isalnum() or c in keep)[:180] or "doc"

def _write_exec_pdf(
    *,
    out_path: pathlib.Path,
    title: str,
    subtitle: str,
    fields: list[tuple[str, str]],
):
    c = canvas.Canvas(str(out_path), pagesize=letter)
    w, h = letter

    # header bar
    c.setFillColor(colors.HexColor("#0b0f14"))
    c.rect(0, h-0.9*inch, w, 0.9*inch, fill=1, stroke=0)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(0.75*inch, h-0.55*inch, title)
    c.setFont("Helvetica", 10)
    c.setFillColor(colors.HexColor("#c7d2e0"))
    c.drawString(0.75*inch, h-0.78*inch, subtitle)

    # body
    y = h - 1.3*inch
    c.setFillColor(colors.black)
    c.setFont("Helvetica", 10)

    for label, value in fields:
        if y < 1.0*inch:
            c.showPage()
            y = h - 1.0*inch
            c.setFont("Helvetica", 10)
        c.setFillColor(colors.HexColor("#111826"))
        c.setFont("Helvetica-Bold", 10)
        c.drawString(0.75*inch, y, f"{label}:")
        c.setFillColor(colors.black)
        c.setFont("Helvetica", 10)
        c.drawString(2.3*inch, y, value or "")
        y -= 0.28*inch

    # footer
    c.setFont("Helvetica", 9)
    c.setFillColor(colors.HexColor("#6b7a90"))
    c.drawRightString(w-0.75*inch, 0.5*inch, f"Generated by Atlas • {utcnow().isoformat()}")
    c.save()

def _store_document_row(
    *,
    conn,
    owner_id: str,
    onboarding_id: str | None,
    doc_type: str,
    doc_version: str,
    filename: str,
    stored_path: pathlib.Path,
    sha256: str,
):
    conn.execute(text("""
        insert into atlas_documents
          (owner_id, onboarding_id, doc_type, doc_version, filename, sha256, stored_path)
        values
          (:owner_id::uuid, :onboarding_id::uuid, :doc_type, :doc_version, :filename, :sha256, :stored_path)
    """), {
        "owner_id": owner_id,
        "onboarding_id": onboarding_id,
        "doc_type": doc_type,
        "doc_version": doc_version,
        "filename": filename,
        "sha256": sha256,
        "stored_path": str(stored_path),
    })

def sha256_file(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

@app.post("/api/vault/ingest")
async def vault_ingest(
    request: Request,
    source_key: str = Form(...),
    org_id: str = Form(""),
    tenant_id: str = Form(""),
    schema_version: str = Form(""),
    manifest_json: str = Form(""),
    bundle: UploadFile = File(...),
):
    actor = require_admin(request)

    filename = bundle.filename or "bundle.bin"
    safe_name = "".join([c for c in filename if c.isalnum() or c in "._-"])[:180]
    ts = utcnow().strftime("%Y%m%dT%H%M%SZ")
    stored_name = f"{source_key}_{ts}_{safe_name}"
    stored_path = VAULT_DIR / stored_name

    size = 0
    with stored_path.open("wb") as out:
        while True:
            chunk = await bundle.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
            size += len(chunk)

    sha = sha256_file(stored_path)

    manifest = {}
    if manifest_json.strip():
        try:
            manifest = json.loads(manifest_json)
        except Exception:
            raise HTTPException(status_code=400, detail="manifest_json must be valid JSON")

    eng = get_engine()
    with eng.begin() as conn:
        s = conn.execute(
            text("select source_key from vault_sources where source_key=:k"),
            {"k": source_key},
        ).fetchone()
        if not s:
            raise HTTPException(status_code=400, detail="unknown source_key (add to vault_sources first)")

        row = conn.execute(text("""
            insert into vault_objects
              (source_key, org_id, tenant_id, schema_version,
               filename, content_type, byte_size, sha256, manifest_json, stored_path)
            values
              (:source_key, :org_id, :tenant_id, :schema_version,
               :filename, :content_type, :byte_size, :sha256, :manifest::jsonb, :stored_path)
            returning id
        """), {
            "source_key": source_key,
            "org_id": org_id,
            "tenant_id": tenant_id,
            "schema_version": schema_version,
            "filename": safe_name,
            "content_type": bundle.content_type or "",
            "byte_size": size,
            "sha256": sha,
            "manifest": json.dumps(manifest),
            "stored_path": str(stored_path),
        }).fetchone()
        object_id = str(row[0])

        conn.execute(text("""
            insert into vault_access_logs (object_id, actor_email, action, ip_address, user_agent)
            values (:oid::uuid, :actor, 'ingest', :ip, :ua)
        """), {
            "oid": object_id,
            "actor": actor,
            "ip": client_ip(request),
            "ua": request.headers.get("user-agent", "")
        })

    return {"ok": True, "object_id": object_id, "sha256": sha, "byte_size": size}

@app.get("/api/admin/vault/objects")
async def list_vault_objects(
    request: Request,
    source_key: str = Query("all"),
    limit: int = Query(50, ge=1, le=200),
):
    actor = require_admin(request)
    eng = get_engine()
    with eng.begin() as conn:
        rows = conn.execute(text("""
          select id, created_at, source_key, org_id, tenant_id, schema_version,
                 filename, byte_size, sha256
          from vault_objects
          where (:k='all' or source_key=:k)
          order by created_at desc
          limit :limit
        """), {"k": source_key, "limit": limit}).fetchall()

        conn.execute(text("""
          insert into vault_access_logs (object_id, actor_email, action, ip_address, user_agent)
          values (null, :actor, 'list_vault', :ip, :ua)
        """), {
            "actor": actor,
            "ip": client_ip(request),
            "ua": request.headers.get("user-agent", "")
        })

    return {"ok": True, "items": [dict(r._mapping) for r in rows]}

@app.get("/api/admin/vault/objects/{object_id}/download")
async def download_vault_object(object_id: str, request: Request):
    actor = require_admin(request)
    eng = get_engine()
    with eng.begin() as conn:
        r = conn.execute(text("""
          select id, filename, stored_path, content_type
          from vault_objects
          where id = :id
        """), {"id": object_id}).fetchone()

        if not r:
            raise HTTPException(status_code=404, detail="Not found")

        conn.execute(text("""
          insert into vault_access_logs (object_id, actor_email, action, ip_address, user_agent)
          values (:oid::uuid, :actor, 'download', :ip, :ua)
        """), {
            "oid": object_id,
            "actor": actor,
            "ip": client_ip(request),
            "ua": request.headers.get("user-agent", "")
        })

    path = pathlib.Path(r.stored_path)
    if not path.exists():
        raise HTTPException(status_code=500, detail="Stored file missing on server")

    return FileResponse(str(path), media_type=r.content_type or "application/octet-stream", filename=r.filename)


# ----------------------------
# Admin: Core CRUD (owners + assets)
# ----------------------------

@app.post("/api/admin/owners")
async def create_owner(payload: Dict[str, Any], request: Request):
    require_admin(request)
    eng = get_engine()
    with eng.begin() as conn:
        row = conn.execute(text("""
          insert into ip_owners (legal_name, entity_type, jurisdiction, address, email, phone)
          values (:legal_name, :entity_type, :jurisdiction, :address, :email, :phone)
          returning id
        """), {
            "legal_name": (payload.get("legal_name") or "").strip(),
            "entity_type": (payload.get("entity_type") or "").strip(),
            "jurisdiction": (payload.get("jurisdiction") or "").strip(),
            "address": (payload.get("address") or "").strip(),
            "email": (payload.get("email") or "").strip(),
            "phone": (payload.get("phone") or "").strip(),
        }).fetchone()
    return {"ok": True, "id": str(row[0])}

@app.get("/api/admin/owners")
async def list_owners(request: Request, limit: int = Query(50, ge=1, le=200)):
    require_admin(request)
    eng = get_engine()
    with eng.begin() as conn:
        rows = conn.execute(text("""
          select id, created_at, legal_name, entity_type, jurisdiction, email
          from ip_owners
          order by created_at desc
          limit :limit
        """), {"limit": limit}).fetchall()
    return {"ok": True, "items": [dict(r._mapping) for r in rows]}

@app.post("/api/admin/assets")
async def create_asset(payload: Dict[str, Any], request: Request):
    require_admin(request)
    eng = get_engine()
    with eng.begin() as conn:
        row = conn.execute(text("""
          insert into ip_assets
            (owner_id, title, asset_type, jurisdictions, reg_no, status, priority_date,
             inventors, current_owner_entity, encumbrances, description, targets)
          values
            (:owner_id::uuid, :title, :asset_type, :jurisdictions, :reg_no, :status, nullif(:priority_date,'')::date,
             :inventors, :current_owner_entity, :encumbrances, :description, :targets)
          returning id
        """), {
            "owner_id": payload.get("owner_id") or None,
            "title": (payload.get("title") or "").strip(),
            "asset_type": (payload.get("asset_type") or "").strip(),
            "jurisdictions": (payload.get("jurisdictions") or "").strip(),
            "reg_no": (payload.get("reg_no") or "").strip(),
            "status": (payload.get("status") or "").strip(),
            "priority_date": (payload.get("priority_date") or "").strip(),
            "inventors": (payload.get("inventors") or "").strip(),
            "current_owner_entity": (payload.get("current_owner_entity") or "").strip(),
            "encumbrances": (payload.get("encumbrances") or "").strip(),
            "description": (payload.get("description") or "").strip(),
            "targets": (payload.get("targets") or "").strip(),
        }).fetchone()
    return {"ok": True, "id": str(row[0])}

@app.get("/api/admin/assets")
async def list_assets(request: Request, limit: int = Query(50, ge=1, le=200)):
    require_admin(request)
    eng = get_engine()
    with eng.begin() as conn:
        rows = conn.execute(text("""
          select a.id, a.created_at, a.title, a.asset_type, a.status, a.reg_no,
                 o.legal_name as owner_name
          from ip_assets a
          left join ip_owners o on o.id = a.owner_id
          order by a.created_at desc
          limit :limit
        """), {"limit": limit}).fetchall()
    return {"ok": True, "items": [dict(r._mapping) for r in rows]}

# ----------------------------
# Documents: Owner + Admin
# ----------------------------

@app.get("/api/owner/docs")
async def owner_docs(request: Request, limit: int = Query(200, ge=1, le=500)):
    owner_id = require_owner(request)
    eng = get_engine()
    with eng.begin() as conn:
        rows = conn.execute(text("""
          select id, created_at, doc_type, doc_version, filename, sha256
          from atlas_documents
          where owner_id = :oid::uuid
          order by created_at desc
          limit :limit
        """), {"oid": owner_id, "limit": limit}).fetchall()
    return {"ok": True, "items": [dict(r._mapping) for r in rows]}

@app.get("/api/owner/docs/{doc_id}/download")
async def owner_doc_download(doc_id: str, request: Request):
    owner_id = require_owner(request)
    eng = get_engine()
    with eng.begin() as conn:
        r = conn.execute(text("""
          select id, owner_id, filename, stored_path
          from atlas_documents
          where id = :id::uuid
        """), {"id": doc_id}).fetchone()
    if not r or str(r.owner_id) != str(owner_id):
        raise HTTPException(status_code=404, detail="Not found")
    path = pathlib.Path(r.stored_path)
    if not path.exists():
        raise HTTPException(status_code=500, detail="Stored file missing on server")
    return FileResponse(str(path), media_type="application/pdf", filename=r.filename)

@app.get("/api/admin/docs")
async def admin_docs(request: Request, owner_id: str = Query(""), limit: int = Query(200, ge=1, le=500)):
    require_admin(request)
    eng = get_engine()
    with eng.begin() as conn:
        if owner_id.strip():
            rows = conn.execute(text("""
              select d.id, d.created_at, d.doc_type, d.doc_version, d.filename, d.sha256,
                     o.legal_name as owner_name, o.email as owner_email
              from atlas_documents d
              left join ip_owners o on o.id = d.owner_id
              where d.owner_id = :oid::uuid
              order by d.created_at desc
              limit :limit
            """), {"oid": owner_id, "limit": limit}).fetchall()
        else:
            rows = conn.execute(text("""
              select d.id, d.created_at, d.doc_type, d.doc_version, d.filename, d.sha256,
                     o.legal_name as owner_name, o.email as owner_email
              from atlas_documents d
              left join ip_owners o on o.id = d.owner_id
              order by d.created_at desc
              limit :limit
            """), {"limit": limit}).fetchall()
    return {"ok": True, "items": [dict(r._mapping) for r in rows]}

@app.get("/api/admin/docs/{doc_id}/download")
async def admin_doc_download(doc_id: str, request: Request):
    require_admin(request)
    eng = get_engine()
    with eng.begin() as conn:
        r = conn.execute(text("""
          select id, filename, stored_path
          from atlas_documents
          where id = :id::uuid
        """), {"id": doc_id}).fetchone()
    if not r:
        raise HTTPException(status_code=404, detail="Not found")
    path = pathlib.Path(r.stored_path)
    if not path.exists():
        raise HTTPException(status_code=500, detail="Stored file missing on server")
    return FileResponse(str(path), media_type="application/pdf", filename=r.filename)

@app.get("/health")
async def health():
    return {"ok": True, "service": "atlas", "time": utcnow().isoformat()}
