# qbo_harvester.py
import os, json, csv, pathlib, webbrowser, base64
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlencode, urlparse, parse_qs
from datetime import date, timedelta
import requests
import difflib, re, secrets
from dotenv import load_dotenv
import difflib

# Load .env and ensure keys
load_dotenv()
if not os.getenv("QBO_CLIENT_ID") or not os.getenv("QBO_CLIENT_SECRET"):
    raise SystemExit("Missing QBO_CLIENT_ID / QBO_CLIENT_SECRET in .env")

# debug: prove what loaded
print("QBO_ENV(raw) =", repr(os.getenv("QBO_ENV")))

QBO_ENV = (os.getenv("QBO_ENV") or "production").lower()
if QBO_ENV.startswith("sand"):
    API_BASE = "https://sandbox-quickbooks.api.intuit.com/v3/company"
else:
    API_BASE = "https://quickbooks.api.intuit.com/v3/company"
print("QBO_ENV =", QBO_ENV, "| API_BASE =", API_BASE)

# ====== CONFIG ======
CUSTOMER_NAMES = ["Mervis", "Oscar Winski", "Lewis Salvage"]  # edit or move to .env later
END   = date.today()
START = END - timedelta(days=5*365)

OUT_DIR   = pathlib.Path("qbo_out"); OUT_DIR.mkdir(exist_ok=True)
PDFS_DIR  = OUT_DIR / "qbo_pdfs"; PDFS_DIR.mkdir(exist_ok=True)
CSV_PATH  = OUT_DIR / "bridge_invoices.csv"
TOK_PATH  = OUT_DIR / "qbo_tokens.json"
MAPPING_CSV_PATH = OUT_DIR / "material_map.csv"
UNMAPPED_LOG     = OUT_DIR / "unmapped_materials.csv"

# ===== QuickBooks OAuth/API (.env) =====
CLIENT_ID     = os.getenv("QBO_CLIENT_ID")
CLIENT_SECRET = os.getenv("QBO_CLIENT_SECRET")
REDIRECT_URI  = os.getenv("QBO_REDIRECT_URI")
if not REDIRECT_URI:
    raise SystemExit("Missing QBO_REDIRECT_URI in .env")
SCOPES        = "com.intuit.quickbooks.accounting"
AUTH_URL      = "https://appcenter.intuit.com/connect/oauth2"
TOKEN_URL     = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

# --- Relay (prod) ---
RELAY_BASE = os.getenv("QBO_RELAY_BASE")          
RELAY_AUTH = os.getenv("QBO_RELAY_AUTH")          
RELAY_PEEK = f"{RELAY_BASE}/admin/qbo/peek" if RELAY_BASE else None

# choose host by env (sandbox vs prod)
QBO_ENV = (os.getenv("QBO_ENV") or "production").lower()
if QBO_ENV.startswith("sand"):
    API_BASE = "https://sandbox-quickbooks.api.intuit.com/v3/company"
else:
    API_BASE = "https://quickbooks.api.intuit.com/v3/company"

print("QBO_ENV =", QBO_ENV, "| API_BASE =", API_BASE)

# ===== BRidge config =====
BRIDGE_BUYER_BASE  = os.getenv("BRIDGE_BUYER_BASE", "https://bridge-buyer.onrender.com")
BRIDGE_SELLER_BASE = os.getenv("BRIDGE_SELLER_BASE", "https://scrapfutures.com")
BRIDGE_POST_TARGET = os.getenv("BRIDGE_POST_TARGET", "seller").lower()
BRIDGE_USER        = os.getenv("BRIDGE_USER")
BRIDGE_PASS        = os.getenv("BRIDGE_PASS")
BRIDGE_SELLER      = os.getenv("BRIDGE_SELLER", "Winski Brothers")
POST_TO_BRIDGE     = os.getenv("POST_TO_BRIDGE", "true").lower() in ("1","true","yes")

ENV = os.getenv("ENV", "production").lower()
HARVESTER_DISABLED = os.getenv("HARVESTER_DISABLED", "0") == "1"

def _bridge_base_for_doc(doc_type: str | None = None):
    if BRIDGE_POST_TARGET == "buyer":
        return BRIDGE_BUYER_BASE
    if BRIDGE_POST_TARGET == "seller":
        return BRIDGE_SELLER_BASE
    # fallback: auto-route based on document type
    if doc_type and doc_type.lower() in {"invoice", "payment", "bill"}:
        return BRIDGE_SELLER_BASE
    return BRIDGE_BUYER_BASE
# ----- Bridge config -----

# ===== Material normalization =====
BASE_MATERIAL_MAP = {
    # Ferrous
    "ferrous sale clips": "Clips",
    "clips": "Clips",
    "hms": "HMS",
    "heavy melt": "HMS",
    "hms 5ft": "HMS 5'",
    "hms 5'": "HMS 5'",
    "p&s": "P&S",
    "p&s 5ft": "P&S 5'",
    "p&s 5'": "P&S 5'",
    "shred": "Shred",
    "shred steel": "Shred",
    # Aluminum examples
    "5052 clip": "Clips 5052 Al",
    "6061 clip": "Clips 6061 Al",
    "3003 clip": "Clips 3003 Al",
    "4017": "4017 Al",
    "acsr & ins alum wire": "Insulated Al Wire",
    "aluminum car wheels": "Al Car Wheels",
    "alum breakage": "Al Breakage",
    "new bare extrusion prepared": "Al Extrusion (Bare)",
}
KEYWORD_RULES = [
    (re.compile(r"\bshred( steel)?\b", re.I), "Shred"),
    (re.compile(r"\bhms\b|\bheavy\s*melt\b", re.I), "HMS"),
    (re.compile(r"\bp\s*&\s*s\b|\bp[/&]s\b", re.I), "P&S"),
    (re.compile(r"\bclips?\b", re.I), "Clips"),
    (re.compile(r"\balum(inum)?\s+car\s+wheels?\b", re.I), "Al Car Wheels"),
    (re.compile(r"\bextrusion\b.*\bbare\b", re.I), "Al Extrusion (Bare)"),
    (re.compile(r"\bins(ulated)?\s+al(uminum)?\s+wire\b|\bacsr\b", re.I), "Insulated Al Wire"),
    (re.compile(r"\bbreakage\b", re.I), "Al Breakage"),
]
CUSTOMER_OVERRIDES = {}  # {"mervis": {"ferrous sale clips":"Clips"}}

def _load_csv_mapping():
    m = {}
    if MAPPING_CSV_PATH.exists():
        with open(MAPPING_CSV_PATH, newline="", encoding="utf-8") as f:
            r = csv.reader(f)
            for row in r:
                if not row or row[0].strip().lower() == "source":  # header
                    continue
                src = row[0].strip().lower()
                can = (row[1] if len(row) > 1 else "").strip()
                if src and can: m[src] = can
    return m
EXTERNAL_MAP = _load_csv_mapping()

def _log_unmapped(src, customer):
    try:
        write_header = not UNMAPPED_LOG.exists()
        with open(UNMAPPED_LOG, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if write_header: w.writerow(["source","customer","suggested"])
            w.writerow([src, customer or "", ""])
    except Exception:
        pass

def normalize_material(source_name: str, customer_name: str | None = None) -> str:
    if not source_name: return "Unknown"
    s = source_name.strip().lower()
    cust = (customer_name or "").strip().lower()

    if cust in CUSTOMER_OVERRIDES and s in CUSTOMER_OVERRIDES[cust]:
        return CUSTOMER_OVERRIDES[cust][s]
    if s in EXTERNAL_MAP:       return EXTERNAL_MAP[s]
    if s in BASE_MATERIAL_MAP:  return BASE_MATERIAL_MAP[s]
    for pat, canon in KEYWORD_RULES:
        if pat.search(source_name): return canon
    keys = list({*BASE_MATERIAL_MAP.keys(), *EXTERNAL_MAP.keys()})
    if keys:
        match = difflib.get_close_matches(s, keys, n=1, cutoff=0.88)
        if match:
            return EXTERNAL_MAP.get(match[0]) or BASE_MATERIAL_MAP.get(match[0]) or source_name
    _log_unmapped(source_name, customer_name)
    return source_name

# ===== OAuth helpers =====
EXPECTED_STATE = None

class OAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global EXPECTED_STATE
        qs = parse_qs(urlparse(self.path).query)

        if "error" in qs:
            err = qs.get("error", [""])[0]
            desc = qs.get("error_description", [""])[0]
            self.send_response(400); self.end_headers()
            self.wfile.write(f"OAuth error: {err} {desc}".encode())
            return

        if "/callback" in self.path and "code" in qs and "realmId" in qs and "state" in qs:
            received_state = qs["state"][0]
            if not EXPECTED_STATE or received_state != EXPECTED_STATE:
                self.send_response(400); self.end_headers()
                self.wfile.write(b"Invalid or missing state parameter.")
                return

            self.server.auth_code = qs["code"][0]
            self.server.realm_id  = qs["realmId"][0]
            self.send_response(200); self.end_headers()
            self.wfile.write(b"You can close this tab.")
        else:
            self.send_response(400); self.end_headers()
            self.wfile.write(b"Missing required query parameters.")

def _basic_auth_header():
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()
    return {
        "Authorization": f"Basic {b64_auth}",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }

def _local_server():
    class LocalServer(HTTPServer):
        def __init__(self, server_address, RequestHandlerClass):
            super().__init__(server_address, RequestHandlerClass)
            self.auth_code = None
            self.realm_id = None
    server = LocalServer(("localhost", 5055), OAuthHandler)
    while server.auth_code is None or server.realm_id is None:
        server.handle_request()
    return server.auth_code, server.realm_id


def oauth_flow():
    global EXPECTED_STATE
    EXPECTED_STATE = secrets.token_urlsafe(32)

    params = {
        "client_id": CLIENT_ID,
        "scope": SCOPES,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "state": EXPECTED_STATE,
    }

    auth_url = f"{AUTH_URL}?{urlencode(params)}"
    print("Open this in a browser if it doesn't launch automatically:\n", auth_url)
    webbrowser.open_new_tab(auth_url)

    code, realm_id = _local_server()

    data = {"grant_type": "authorization_code", "code": code, "redirect_uri": REDIRECT_URI}
    r = requests.post(TOKEN_URL, headers=_basic_auth_header(), data=data, timeout=30)
    r.raise_for_status()
    tokens = r.json()
    tokens["realmId"] = realm_id
    TOK_PATH.write_text(json.dumps(tokens, indent=2))
    return tokens

def oauth_flow_via_relay():
    """
    Production OAuth using the hosted relay:
      1) open Intuit auth to REDIRECT_URI at your relay (/qbo/callback)
      2) poll RELAY_PEEK?state=... with X-Relay-Auth
      3) exchange code->tokens locally; save to qbo_out/qbo_tokens.json
    """
    if not (RELAY_BASE and RELAY_AUTH and REDIRECT_URI and REDIRECT_URI.startswith("https://")):
        raise SystemExit("Relay not configured: set QBO_RELAY_BASE, QBO_RELAY_AUTH, and a https REDIRECT_URI")

    global EXPECTED_STATE
    EXPECTED_STATE = secrets.token_urlsafe(32)

    params = {
        "client_id": CLIENT_ID,
        "scope": SCOPES,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "state": EXPECTED_STATE,
    }
    auth_url = f"{AUTH_URL}?{urlencode(params)}"
    print("Open this in a browser if it doesn't launch automatically:\n", auth_url)
    webbrowser.open_new_tab(auth_url)

    # Poll the relay for code/realmId (small backoff)
    for _ in range(120):  # ~2 minutes
        try:
            r = requests.get(RELAY_PEEK, params={"state": EXPECTED_STATE},
                             headers={"X-Relay-Auth": RELAY_AUTH}, timeout=6)
            if r.status_code == 200:
                payload = r.json()
                code     = payload["code"]
                realm_id = payload["realmId"]
                data = {"grant_type": "authorization_code", "code": code, "redirect_uri": REDIRECT_URI}
                tr = requests.post(TOKEN_URL, headers=_basic_auth_header(), data=data, timeout=30)
                tr.raise_for_status()
                tokens = tr.json()
                tokens["realmId"] = realm_id
                TOK_PATH.write_text(json.dumps(tokens, indent=2))
                return tokens
        except Exception:
            pass
        _t = 0.5
        try:
            import time as _time
            _time.sleep(_t)
        except Exception:
            pass

    raise SystemExit("OAuth timed out waiting for relay; re-run and approve quickly.")

def _refresh_tokens(toks):
    data = {"grant_type":"refresh_token", "refresh_token": toks["refresh_token"]}
    r = requests.post(TOKEN_URL, headers=_basic_auth_header(), data=data, timeout=30)
    r.raise_for_status()
    new_tokens = r.json()
    new_tokens["realmId"] = toks["realmId"]
    TOK_PATH.write_text(json.dumps(new_tokens, indent=2))
    return new_tokens

def get_tokens():
    if TOK_PATH.exists():
        try:
            return json.loads(TOK_PATH.read_text())
        except Exception:
            pass
    print("No valid token file found. Starting OAuth flow...")
    # Use relay if configured (prod), else local callback server (sandbox/dev)
    if RELAY_BASE and RELAY_AUTH and REDIRECT_URI.startswith("https://"):
        return oauth_flow_via_relay()
    return oauth_flow()

# ===== QBO query & PDF =====
def api_headers(access_token):
    return {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

def pdf_headers(access_token):
    return {"Authorization": f"Bearer {access_token}", "Accept": "application/pdf"}

def qbo_query(toks, query):
    url = f"{API_BASE}/{toks['realmId']}/query"
    params = {"query": query, "minorversion": "73"}
    r = requests.get(url, headers=api_headers(toks["access_token"]), params=params)
    if r.status_code == 401:
        toks = _refresh_tokens(toks)
        r = requests.get(url, headers=api_headers(toks["access_token"]), params=params)

    if r.status_code >= 400:
        tid = r.headers.get("intuit_tid")
        msg = r.text[:2000]
        print(f"[QBO ERROR] {r.status_code} tid={tid} body={msg}")
        r.raise_for_status()

    return r.json(), toks

def get_invoices_for_customer(toks, cust_id, start_date, end_date):
    all_invs, startpos = [], 1
    while True:
        q = (
          "SELECT Id, DocNumber, TxnDate, TotalAmt, Balance, CustomerRef, ShipDate, "
          "ShipMethodRef, Line "
          f"FROM Invoice WHERE CustomerRef = '{cust_id}' "
          f"AND TxnDate >= '{start_date}' AND TxnDate <= '{end_date}' "
          f"ORDER BY TxnDate STARTPOSITION {startpos} MAXRESULTS 500"
        )
        data, toks = qbo_query(toks, q)
        invs = data.get("QueryResponse", {}).get("Invoice", [])
        if not invs: break
        all_invs.extend(invs); startpos += len(invs)
    return all_invs, toks

def download_invoice_pdf(toks, invoice_id, out_path: pathlib.Path):
    """
    Downloads a QBO invoice PDF with 401 refresh retry and robust error logging.
    Writes to `out_path` atomically.
    """
    url = f"{API_BASE}/{toks['realmId']}/invoice/{invoice_id}/pdf"

    def _do_get(toks_):
        return requests.get(
            url,
            headers=pdf_headers(toks_["access_token"]),
            stream=True,
            timeout=30,
        )

    r = _do_get(toks)
    if r.status_code == 401:
        toks = _refresh_tokens(toks)
        r = _do_get(toks)

    if r.status_code >= 400:
        tid = r.headers.get("intuit_tid")
        # Print a short, support-friendly line; then raise
        print(f"[QBO PDF ERROR] {r.status_code} tid={tid} invoice_id={invoice_id} body={r.text[:500]}")
        r.raise_for_status()

    # Success: stream to a temp file then move into place
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".part")
    with open(tmp_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    tmp_path.replace(out_path)
    return toks

# ===== BRidge client =====
def bridge_login(session: requests.Session) -> None:
    if not (BRIDGE_USER and BRIDGE_PASS):
        return
    r = session.post(f"{BRIDGE_BASE}/login",
                     json={"username": BRIDGE_USER, "password": BRIDGE_PASS},
                     timeout=15)
    if r.status_code != 200:
        raise RuntimeError(f"BRidge login failed: {r.status_code} {r.text}")

def post_contract_to_bridge(session: requests.Session, row: dict, seller_name: str = "Winski Brothers"):
    # derive tonnage & $/ton from the QBO line
    qty = row.get("qty")
    uom = (row.get("uom") or "").lower()
    unit_price = row.get("unit_price")
    if qty is None or unit_price is None:
        return

    if uom in ("lb", "lbs", "pound", "pounds"):
        weight_tons = float(qty) / 2000.0
        price_per_ton = float(unit_price) * 2000.0
    elif uom in ("ton", "tons", "t"):
        weight_tons = float(qty)
        price_per_ton = float(unit_price)
    else:
        weight_tons = float(qty) / 2000.0
        price_per_ton = float(unit_price) * 2000.0

    # canonical material + provenance
    material_canon = normalize_material(row.get("item_original") or row.get("item") or "", row.get("customer"))
    payload = {
        "buyer": row["customer"],
        "seller": seller_name,
        "material": material_canon,
        "weight_tons": round(weight_tons, 6),
        "price_per_ton": round(price_per_ton, 2),
        "pricing_formula": None,
        "reference_symbol": f"{row.get('invoice_number')}#{row.get('_line_index', 0)}",
        "reference_price": None,
        "reference_source": "QBO",
        "reference_timestamp": row.get("invoice_date"),
        "currency": "USD",
        # "meta": {"qbo_item": row.get("item_original"), "qbo_uom": row.get("uom")}
    }

    # headers (idempotency + optional historical import mode)
    headers = {
        "Idempotency-Key": f"QBO:{payload['reference_symbol']}",
    }
    if os.getenv("BRIDGE_IMPORT_MODE", "historical").lower() == "historical":
        headers["X-Import-Mode"] = "historical"

        if ENV in {"ci", "test"} or HARVESTER_DISABLED:
            print("[bridge] Skipped (CI/test mode)")
            return {"ok": True, "stub": True}

        base = _bridge_base_for_doc("invoice")
        url  = f"{base.rstrip('/')}/contracts"
        r = session.post(url, json=payload, timeout=25, headers=headers)

# --- Dump all customers for visibility -------------------------------------------------
def dump_customers_csv(toks, out_path="qbo_out/customers.csv"):
    all_rows, startpos = [], 1
    while True:
        q = f"SELECT Id, DisplayName FROM Customer STARTPOSITION {startpos} MAXRESULTS 500"
        data, toks = qbo_query(toks, q)
        rows = data.get("QueryResponse", {}).get("Customer", [])
        if not rows:
            break
        all_rows.extend(rows)
        startpos += len(rows)

    import csv, os
    os.makedirs("qbo_out", exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["Id","DisplayName"])
        for r in all_rows:
            w.writerow([r.get("Id",""), r.get("DisplayName","")])
    print(f"Customers → {out_path}  (total: {len(all_rows)})")
    return toks


# --- Forgiving customer lookup: EXACT → LIKE → FUZZY -----------------------------------
def get_customer_id_by_name(toks, display_name):
    import difflib
    target = (display_name or "").strip()
    safe = target.replace("'", "''")

    # 1) Exact
    q = f"SELECT Id, DisplayName FROM Customer WHERE DisplayName = '{safe}'"
    data, toks = qbo_query(toks, q)
    rows = data.get("QueryResponse", {}).get("Customer", [])
    if rows:
        print(f"[match:EXACT] '{target}' → '{rows[0]['DisplayName']}' (Id={rows[0]['Id']})")
        return rows[0]["Id"], toks

    # 2) LIKE (contains)
    q_like = f"SELECT Id, DisplayName FROM Customer WHERE DisplayName LIKE '%{safe}%'"
    data, toks = qbo_query(toks, q_like)
    rows = data.get("QueryResponse", {}).get("Customer", [])
    if rows:
        names = [r["DisplayName"] for r in rows if r.get("DisplayName")]
        best = difflib.get_close_matches(target, names, n=1, cutoff=0.6)
        chosen = next((r for r in rows if best and r["DisplayName"] == best[0]), rows[0])
        print(f"[match:LIKE]  '{target}' → '{chosen['DisplayName']}' (Id={chosen['Id']})")
        return chosen["Id"], toks

    # 3) FUZZY against first 1000
    data, toks = qbo_query(toks, "SELECT Id, DisplayName FROM Customer MAXRESULTS 1000")
    rows = data.get("QueryResponse", {}).get("Customer", [])
    if rows:
        names = [r["DisplayName"] for r in rows if r.get("DisplayName")]
        best = difflib.get_close_matches(target, names, n=1, cutoff=0.6)
        if best:
            chosen = next(r for r in rows if r["DisplayName"] == best[0])
            print(f"[match:FUZZY] '{target}' → '{chosen['DisplayName']}' (Id={chosen['Id']})")
            return chosen["Id"], toks

    print(f"[WARN] Customer not found (after fuzzy): {target}")
    return None, toks


# --- Fallback: pull ALL invoices without customer filter -------------------------------
def get_invoices_all(toks, start_date, end_date):
    all_invs, startpos = [], 1
    while True:
        q = (
            "SELECT Id, DocNumber, TxnDate, TotalAmt, Balance, CustomerRef, ShipDate, "
            "ShipMethodRef, Line "
            f"FROM Invoice WHERE TxnDate >= '{start_date}' AND TxnDate <= '{end_date}' "
            f"ORDER BY TxnDate STARTPOSITION {startpos} MAXRESULTS 500"
        )
        data, toks = qbo_query(toks, q)
        invs = data.get("QueryResponse", {}).get("Invoice", [])
        if not invs: break
        all_invs.extend(invs); startpos += len(invs)
    return all_invs, toks

# ===== Main =====
def get_tokens():
    if TOK_PATH.exists():
        try:
            return json.loads(TOK_PATH.read_text())
        except Exception:
            pass
    print("No valid token file found. Starting OAuth flow...")
    return oauth_flow()

def main():
    toks = get_tokens()

    # NEW: dump all customers (helps pick exact names later)
    toks = dump_customers_csv(toks)

    rows = []
    sess = requests.Session()
    # If you truly don’t want to post to BRidge, make sure POST_TO_BRIDGE=False up top.

    matched_any = False
    for name in CUSTOMER_NAMES:
        cust_id, toks = get_customer_id_by_name(toks, name)
        if not cust_id:
            continue  # keep going; we’ll fallback if none match

        matched_any = True
        invs, toks = get_invoices_for_customer(toks, cust_id, START.isoformat(), END.isoformat())
        print(f"[{name}] {len(invs)} invoices")

        cdir = PDFS_DIR / name.replace(" ", "_"); cdir.mkdir(parents=True, exist_ok=True)

        for inv in invs:
            inv_id   = inv["Id"]
            doc_no   = inv.get("DocNumber")
            inv_date = inv.get("TxnDate")
            total    = inv.get("TotalAmt")
            balance  = inv.get("Balance")
            ship_dt  = inv.get("ShipDate")
            ship_m   = (inv.get("ShipMethodRef") or {}).get("name")

            pdf_path = cdir / f"INV-{inv_id}.pdf"
            if not pdf_path.exists():
                toks = download_invoice_pdf(toks, inv_id, pdf_path)

            for idx, L in enumerate(inv.get("Line", [])):
                if L.get("DetailType") != "SalesItemLineDetail":
                    continue
                d = L.get("SalesItemLineDetail", {})
                item_name = (d.get("ItemRef") or {}).get("name")
                qty       = d.get("Qty")
                unitprice = d.get("UnitPrice")
                amount    = L.get("Amount")
                uom       = d.get("UnitOfMeasure")

                material_canon = normalize_material(item_name or "", customer_name=(inv.get("CustomerRef") or {}).get("name"))

                row = {
                    "customer": (inv.get("CustomerRef") or {}).get("name") or name,
                    "invoice_id": inv_id,
                    "invoice_number": doc_no,
                    "invoice_date": inv_date,
                    "ship_date": ship_dt,
                    "ship_via": ship_m,
                    "item": material_canon,
                    "item_original": item_name,
                    "qty": qty,
                    "uom": uom,
                    "unit_price": unitprice,
                    "line_amount": amount,
                    "invoice_total": total,
                    "invoice_balance": balance,
                    "pdf_path": str(pdf_path),
                    "_line_index": idx,
                }
                rows.append(row)

                if POST_TO_BRIDGE:
                    try:
                        post_contract_to_bridge(sess, row, seller_name=BRIDGE_SELLER)
                    except Exception as e:
                        print(f"[bridge] post error: {e}")

    # Fallback: no customer matched → pull everything to prove pipeline
    if not matched_any:
        print("[fallback] No CUSTOMER_NAMES matched. Pulling ALL invoices in date window…")
        invs, toks = get_invoices_all(toks, START.isoformat(), END.isoformat())
        print(f"[ALL CUSTOMERS] {len(invs)} invoices")
        cdir = PDFS_DIR / "_ALL"; cdir.mkdir(parents=True, exist_ok=True)
        for inv in invs:
            inv_id   = inv["Id"]
            doc_no   = inv.get("DocNumber")
            inv_date = inv.get("TxnDate")
            total    = inv.get("TotalAmt")
            balance  = inv.get("Balance")
            custname = (inv.get("CustomerRef") or {}).get("name")
            ship_dt  = inv.get("ShipDate")
            ship_m   = (inv.get("ShipMethodRef") or {}).get("name")

            pdf_path = cdir / f"INV-{inv_id}.pdf"
            if not pdf_path.exists():
                toks = download_invoice_pdf(toks, inv_id, pdf_path)

            for idx, L in enumerate(inv.get("Line", [])):
                if L.get("DetailType") != "SalesItemLineDetail":
                    continue
                d = L.get("SalesItemLineDetail", {})
                item_name = (d.get("ItemRef") or {}).get("name")
                qty       = d.get("Qty")
                unitprice = d.get("UnitPrice")
                amount    = L.get("Amount")
                uom       = d.get("UnitOfMeasure")

                material_canon = normalize_material(item_name or "", customer_name=custname)

                row = {
                    "customer": custname,
                    "invoice_id": inv_id,
                    "invoice_number": doc_no,
                    "invoice_date": inv_date,
                    "ship_date": ship_dt,
                    "ship_via": ship_m,
                    "item": material_canon,
                    "item_original": item_name,
                    "qty": qty,
                    "uom": uom,
                    "unit_price": unitprice,
                    "line_amount": amount,
                    "invoice_total": total,
                    "invoice_balance": balance,
                    "pdf_path": str(pdf_path),
                    "_line_index": idx,
                }
                rows.append(row)

                if POST_TO_BRIDGE:
                    try:
                        post_contract_to_bridge(sess, row, seller_name=BRIDGE_SELLER)
                    except Exception as e:
                        print(f"[bridge] post error: {e}")

    headers = [
        "customer","invoice_id","invoice_number","invoice_date","ship_date","ship_via",
        "item","item_original","qty","uom","unit_price","line_amount","invoice_total",
        "invoice_balance","pdf_path"
    ]
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader(); w.writerows(rows)

    print(f"Done → {CSV_PATH}  | PDFs under {PDFS_DIR}")

if __name__ == "__main__":
    main()
