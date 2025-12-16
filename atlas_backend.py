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


@app.post("/api/auth/login")
async def login(payload: Dict[str, Any], request: Request):
    email = (payload.get("email") or "").lower().strip()
    password = (payload.get("password") or "").strip()
    if not email or not password:
        raise HTTPException(status_code=400, detail="email and password required")

    eng = get_engine()
    with eng.begin() as conn:
        r = conn.execute(
            text("select email, password_hash, role from atlas_users where email=:e"),
            {"e": email},
        ).fetchone()

    if not r:
        raise HTTPException(status_code=401, detail="invalid credentials")
    if not bcrypt.checkpw(password.encode("utf-8"), r.password_hash.encode("utf-8")):
        raise HTTPException(status_code=401, detail="invalid credentials")

    request.session["email"] = r.email
    request.session["role"] = r.role
    return {"ok": True, "email": r.email, "role": r.role}

@app.post("/api/auth/logout")
async def logout(request: Request):
    request.session.clear()
    return {"ok": True}

@app.get("/api/auth/me")
async def me(request: Request):
    return {"email": request.session.get("email"), "role": request.session.get("role")}


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
               nda_json, intake_json, attestation_json, participation_json, billing_ack_json, doc_versions_json,
               ip_assets_count, user_agent, ip_address
            values
              (:owner_email, :owner_name, :entity_type, :jurisdiction,
               :nda_json::jsonb, :intake_json::jsonb, :attestation_json::jsonb, :participation_json::jsonb, :billing_ack_json::jsonb, :doc_versions_json::jsonb,
               :ip_assets_count, :user_agent, :ip_address
            returning id
        """), {
            "owner_email": owner_email,
            "owner_name": owner_name,
            "entity_type": entity_type,
            "jurisdiction": jurisdiction,
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

@app.get("/health")
async def health():
    return {"ok": True, "service": "atlas", "time": utcnow().isoformat()}
