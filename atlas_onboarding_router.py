from __future__ import annotations
from __future__ import annotations

# --- stdlib ---
import os
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

# --- third-party ---
import bcrypt

from fastapi import (
    FastAPI,
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from fastapi.responses import (
    HTMLResponse,
    FileResponse,
    JSONResponse,
    RedirectResponse,
)
from fastapi.staticfiles import StaticFiles

from fastapi.middleware.cors import CORSMiddleware  # ✅ correct (NOT starlette.middleware.cors)
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from pydantic import BaseModel, EmailStr, Field

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# You already have an engine pattern in BRidge.
# Replace get_engine() with your existing engine getter.
def get_engine() -> Engine:
    from sqlalchemy import create_engine
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return create_engine(dsn, pool_pre_ping=True)

router = APIRouter(prefix="/api/atlas", tags=["Atlas Onboarding"])

class OnboardingPayload(BaseModel):
    owner: Dict[str, Any] = Field(default_factory=dict)
    nda: Dict[str, Any] = Field(default_factory=dict)
    intake: Dict[str, Any] = Field(default_factory=dict)
    attestation: Dict[str, Any] = Field(default_factory=dict)
    participation: Dict[str, Any] = Field(default_factory=dict)
    billing_ack: Dict[str, Any] = Field(default_factory=dict)

@router.post("/onboarding")
async def submit_onboarding(payload: OnboardingPayload, request: Request):
    owner = payload.owner or {}
    owner_email = owner.get("email")
    owner_name = owner.get("legal_name") or owner.get("name")
    entity_type = owner.get("entity_type")
    jurisdiction = owner.get("jurisdiction")
    ip_assets = (payload.intake or {}).get("ip_assets") or []
    ip_assets_count = len(ip_assets)

    # minimal sanity
    if not owner_email:
        raise HTTPException(status_code=400, detail="owner.email is required")
    if not owner_name:
        raise HTTPException(status_code=400, detail="owner.legal_name is required")
    if ip_assets_count < 1:
        raise HTTPException(status_code=400, detail="At least 1 IP asset is required")

    ua = request.headers.get("user-agent", "")
    ip_addr = request.client.host if request.client else None

    eng = get_engine()
    with eng.begin() as conn:
        row = conn.execute(text("""
            insert into atlas_onboarding_submissions
              (owner_email, owner_name, entity_type, jurisdiction,
               nda_json, intake_json, attestation_json, participation_json, billing_ack_json,
               ip_assets_count, user_agent, ip_address)
            values
              (:owner_email, :owner_name, :entity_type, :jurisdiction,
               :nda_json::jsonb, :intake_json::jsonb, :attestation_json::jsonb, :participation_json::jsonb, :billing_ack_json::jsonb,
               :ip_assets_count, :user_agent, :ip_address)
            returning id
        """), {
            "owner_email": owner_email,
            "owner_name": owner_name,
            "entity_type": entity_type,
            "jurisdiction": jurisdiction,
            "nda_json": json.dumps(payload.nda),
            "intake_json": json.dumps(payload.intake),
            "attestation_json": json.dumps(payload.attestation),
            "participation_json": json.dumps(payload.participation),
            "billing_ack_json": json.dumps(payload.billing_ack),
            "ip_assets_count": ip_assets_count,
            "user_agent": ua,
            "ip_address": ip_addr
        }).fetchone()
        onboarding_id = str(row[0])

    return {
        "ok": True,
        "onboarding_id": onboarding_id,
        "status": "submitted"
    }

@router.get("/onboarding/{onboarding_id}")
async def get_onboarding(onboarding_id: str):
    eng = get_engine()
    with eng.begin() as conn:
        r = conn.execute(text("""
            select id, created_at, status,
                   owner_email, owner_name, entity_type, jurisdiction,
                   nda_json, intake_json, attestation_json, participation_json, billing_ack_json,
                   ip_assets_count
            from atlas_onboarding_submissions
            where id = :id
        """), {"id": onboarding_id}).fetchone()

    if not r:
        raise HTTPException(status_code=404, detail="Not found")

    return {
        "id": str(r.id),
        "created_at": r.created_at.isoformat(),
        "status": r.status,
        "owner": {
            "email": r.owner_email,
            "legal_name": r.owner_name,
            "entity_type": r.entity_type,
            "jurisdiction": r.jurisdiction
        },
        "nda": r.nda_json,
        "intake": r.intake_json,
        "attestation": r.attestation_json,
        "participation": r.participation_json,
        "billing_ack": r.billing_ack_json,
        "ip_assets_count": r.ip_assets_count
    }
