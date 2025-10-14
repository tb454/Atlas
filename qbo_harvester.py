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
CUSTOMER_NAMES = [
    "Mervis",
    "Oscar Winski",
    "Lewis Salvage",
    "Josh Padnos",
    "Frymans Recycling #1",
    "Rochester Iron & Metal",
    "48forty Solutions",
    "Alloys Tech",
    "C&Y Global (Pro Metal Recycling)",
    "Farnsworth Metal Recycling",
    "J. Solotken & Company",
    "MDK ZeroLandfill",
    "Newco Metal",
    "Nucor (DJJ or David Joseph)",
    "SDI (Steel Dynamics)",
    "Stainless Steel Midwest",
    "Storage Solutions",
    "Werner & Son",
]
END   = date.today()
START = END - timedelta(days=100*365)

OUT_DIR   = pathlib.Path("qbo_out"); OUT_DIR.mkdir(exist_ok=True)
PDFS_DIR  = OUT_DIR / "qbo_pdfs"; PDFS_DIR.mkdir(exist_ok=True)
CSV_PATH  = OUT_DIR / "bridge_invoices.csv"
CONTRACTS_CSV_PATH   = OUT_DIR / "bridge_contracts.csv"
CONTRACTS_JSONL_PATH = OUT_DIR / "bridge_contracts.ndjson"
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
    # ---- Core ferrous you started with ----
    "ferrous sale clips": "Clips",
    "clips": "Clips",
    "hms": "HMS",
    "heavy melt": "HMS",
    "hms 5ft": "HMS 5'",
    "hms 5'": "HMS 5'",
    "p&s": "P&S",
    "p & s": "P&S",
    "p&s 5ft": "P&S 5'",
    "p&s 5'": "P&S 5'",
    "shred": "Shred",
    "shred steel": "Shred",
    "busheling": "Busheling",
    "busheling/prime": "Busheling",
    "old sheet": "Old Sheet",
    "old sheet aluminum": "Old Sheet Aluminum",
    "low grade sheet iron": "Shred",
    "drums and rotors": "Drums & Rotors",
    "rotors / drums": "Drums & Rotors",
    "unprepared": "Unprepared HMS",
    "unprepared hms": "Unprepared HMS",
    "unprepared heavy melt": "Unprepared HMS",
    "unprepared fe": "HMS",
    "unprepared pipe": "Unprepared HMS",
    "pipe from solar farm": "Unprepared HMS",
    "pipe from solar farm project": "Unprepared HMS",
    "rebar": "Rebar",
    "bolts & nuts": "Bolts & Nuts",
    "substation boxes": "Shred",
    "old sheet dumpster": "Old Sheet Aluminum",
    "old sheet alum dumpster": "Old Sheet Aluminum",
    "stainless steel": "Stainless",
    "ss bkg": "Stainless Breakage",
    "al bkg": "Al Breakage",
    "alum breakage": "Al Breakage",
    "aluminum car wheels": "Al Car Wheels",
    "new bare extrusion prepared": "Al Extrusion (Bare)",
    "alum cans": "Alum Cans",
    "aluminum cans": "Alum Cans",
    "acsr & ins alum wire": "Insulated Al Wire",

    # ---- Ferrous spec aliases ----
    "plate and structural": "P&S",
    "plate & structural": "P&S",
    "p&s (spec)": "P&S",
    "mixed #1 / #2 heavy melt": "Mixed #1/#2 HMS",
    "mixed #1/#2 heavy melt": "Mixed #1/#2 HMS",
    "mixed #1 #2 hm": "Mixed #1/#2 HMS",
    "mixed #1/#2 hm": "Mixed #1/#2 HMS",
    "#2 heavy melt": "#2 HMS",
    "no. 2 heavy melt": "#2 HMS",
    "hms #2 (spec)": "#2 HMS",
    "#1 heavy melt": "#1 HMS",
    "no. 1 heavy melt": "#1 HMS",
    "#1 shredded scrap": "Shred #1",
    "#1 shred (spec)": "Shred #1",
    "no. 1 shredded": "Shred #1",
    "#2 shredded scrap": "Shred #2",
    "#2 shred (spec)": "Shred #2",
    "no. 2 shredded": "Shred #2",
    "turnings": "Turnings",
    "#1 bundles": "#1 Bundles",
    "no. 1 bundles": "#1 Bundles",
    "#2 bundles": "#2 Bundles",
    "no. 2 bundles": "#2 Bundles",
    "prepared cast": "Prepared Cast",
    "prepared cast iron": "Prepared Cast",
    "dealer clips": "Dealer Clips / Tin Plate Busheling",
    "tin plate busheling": "Dealer Clips / Tin Plate Busheling",
    "dealer clips, tin plate busheling": "Dealer Clips / Tin Plate Busheling",
    "tire wire": "Tire Wire",
    "pig iron": "Pig Iron",

    # ---- Copper / brass ISRI shortcuts ----
    "barley": "No. 1 Copper Wire (Barley)",
    "berry": "No. 1 Copper Wire (Berry)",
    "birch": "No. 2 Copper Wire (Birch)",
    "candy": "No. 1 Heavy Copper Solids & Tubing (Candy)",
    "berry/candy": "Berry/Candy",
    "cliff": "No. 2 Copper Solids & Tubing (Cliff)",
    "birch/cliff": "Birch/Cliff",
    "clove": "No. 1 Copper Wire Nodules (Clove)",
    "cobra": "No. 2 Copper Wire Nodules (Cobra)",
    "cocoa": "Copper Wire Nodules (Cocoa)",
    "dream": "Light Copper (Dream)",
    "drink": "Refinery Brass (Drink)",
    "droid": "Insulated Copper Wire Scrap (Droid)",
    "drove": "Copper-Bearing Scrap (Drove)",
    "druid": "Insulated Copper Wire Scrap (Druid)",
    "ebony": "Composition / Red Brass (Ebony)",
    "ebulent": "Lead-Free Bismuth Brass Solids (Ebulent)",
    "ecstatic": "Lead-Free Bismuth Brass Turnings (Ecstatic)",
    "eland": "High Grade Low-Lead Bronze/Brass Solids (Eland)",
    "elder": "Genuine Babbitt-Lined Brass Bushings (Elder)",
    "elias": "High-Lead Bronze Solids & Borings (Elias)",

    # ---- Aluminum ISRI ----
    "tablet": "Clean Aluminum Lithographic Sheets (Tablet)",
    "tabloid": "New Clean Aluminum Lithographic Sheets (Tabloid)",
    "taboo": "Mixed Low Copper Aluminum Clips & Solids (Taboo)",
    "taint": "Clean Mixed Old Alloy Sheet Aluminum (Taint/Tabor)",
    "tabor": "Clean Mixed Old Alloy Sheet Aluminum (Taint/Tabor)",
    "take": "New Aluminum Can Stock (Take)",
    "talc": "Post-Consumer Aluminum Cans / UBC (Talc)",
    "talcred": "Shredded UBC (Talcred)",
    "taldack": "Densified UBC (Taldack)",
    "taldon": "Baled UBC (Taldon)",
    "taldork": "Briquetted UBC (Taldork)",

    # ---- Zinc ISRI ----
    "saves": "Old Zinc Die Cast Scrap (Saves)",
    "scabs": "New Zinc Die Cast Scrap (Scabs)",
    "scoot": "Zinc Die Cast Automotive Grilles (Scoot)",
    "scope": "New Plated Zinc Die Cast Scrap (Scope)",
    "score": "Old Scrap Zinc (Score)",
    "screen": "New Zinc Clippings (Screen)",
    "scribe": "Crushed Clean Sorted Fragmentizers Die Cast Scrap (Scribe)",
    "scroll": "Unsorted Zinc Die Cast Scrap (Scroll)",
    "scrub": "Hot Dip Galvanizers Slab Zinc Dross — Batch (Scrub)",
    "scull": "Zinc Die Cast Slabs or Pigs (Scull)",
    "seal": "Continuous Line Galvanizing Slab Zinc Top Dross (Seal)",
    "seam": "Continuous Line Galvanizing Slab Zinc Bottom Dross (Seam)",
    "shelf": "Prime Zinc Die Cast Dross (Shelf)",

    # ---- Magnesium ISRI ----
    "wafer": "Magnesium Clips (Wafer)",
    "walnut": "Magnesium Scrap (Walnut)",
    "wine": "Magnesium Engraver Plates (Wine)",
    "wood": "Magnesium Dockboards (Wood)",
    "world": "Magnesium Turnings (World)",

    # ---- Lead ISRI ----
    "racks": "Scrap Lead — Soft (Racks)",
    "radio": "Mixed Hard/Soft Scrap Lead (Radio)",
    "rains": "Scrap Drained/Dry Whole Intact Lead (Batteries) (Rains)",
    "rakes": "Battery Lugs (Rakes)",
    "reels": "Mixed Nonferrous Wheel Weights (Reels)",
    "relay": "Lead Covered Copper Cable (Relay)",
    "rents": "Lead Dross (Rents)",
    "rink": "Scrap Wet Whole Intact Lead Batteries (Rink)",
    "rono": "Scrap Industrial Intact Lead Cells (Rono)",
    "roper": "Scrap Whole Intact Industrial Lead Batteries (Roper)",
    "ropes": "Lead Wheel Weights (Ropes)",

    # ---- Nickel/Stainless/Hi-temp ISRI ----
    "aroma": "New Nickel Scrap (Aroma)",
    "burly": "Old Nickel Scrap (Burly)",
    "dandy": "New Cupro Nickel Clips & Solids (Dandy)",
    "daunt": "Cupro Nickel Solids (Daunt)",
    "decoy": "Cupro Nickel Turnings & Borings (Decoy)",
    "delta": "Soldered Cupro Nickel Solids (Delta)",
    "depth": "Misc. Nickel-Copper / Nickel-Copper-Iron (Depth)",
    "hitch": "New R-Monel Clippings & Solids (Hitch)",
    "house": "New Mixed Monel Solids & Clippings (House)",
    "ideal": "Old Monel Sheet & Solids (Ideal)",
    "indian": "K-Monel Solids (Indian)",
    "junto": "Soldered Monel Sheet & Solids (Junto)",
    "lemon": "Monel Castings (Lemon)",
    "lemur": "Monel Turnings (Lemur)",
    "pekoe": "200 Series Stainless Steel Scrap Solids (Pekoe)",
    "sabot": "Stainless Steel Scrap (18-8) (Sabot)",
    "saint": "Nickel Bearing Scrap (Saint)",
    "ultra": "Stainless Steel Turnings (Ultra)",
    "vaunt": "Edison Batteries (Nickel-Iron) (Vaunt)",
    "zurik": "Shredded Nonferrous Sensor Sorted Scrap (predom. SS) (Zurik)",

    # ---- Mixed metals / shop pulls ----
    "darth": "Fluorescent Ballasts (Darth)",
    "vader": "Sealed Units / Compressors (Vader)",
    "elmo": "Mixed Electric Motors (Elmo)",
    "small elmo": "Small Electric Motors (Elmo)",
    "sheema": "Shredded Electric Motors / Meatballs (Sheema)",
    "shelmo": "Shredded Electric Motors / Meatballs (Shelmo)",

    # ---- Other ISRI ----
    "ranch": "Block Tin (Ranch)",
    "ranks": "Pewter (Ranks)",
    "raves": "High Tin Base Babbitt (Raves)",
    "roses": "Mixed Common Babbitt (Roses)",
    "sails": "Titanium Scrap (Sails)",
    "sakes": "Titanium Turnings (Sakes)",

    # ---- Your AL 60xx / PAS / MLC set ----
    "al 6061 new bare extrusion prepared": "Al Extrusion (Bare)",
    "al 6061 clip/plate/pipe prepared": "Clips 6061 Al",
    "6061 clip": "Clips 6061 Al",
    "al 6063 secondary prepared": "6063 Secondary",
    "al 6063 old extrusion prepared": "Al Extrusion 6063 (Old)",
    "al 6063 extrusion 10/10 prepared": "Al Extrusion 6063 10/10",
    "al pas baled": "Painted Aluminum Siding (Baled)",
    "al pas bailed": "Painted Aluminum Siding (Baled)",
    "al cast clean": "Al Cast (Clean)",
    "al rads clean /baled": "Al Radiators (Clean, Baled)",
    "al rads dirty/baled": "Al Radiators (Dirty, Baled)",
    "al sheet clean /baled": "Al Sheet (Clean, Baled)",
    "al wheel auto clean": "Al Car Wheels",
    "al wheel auto dirty 2 cents less": "Al Car Wheels (Dirty)",
    "al wheel chrome clean": "Al Car Wheels (Chrome)",
    "al wheel chrome dirty 2 cents less": "Al Car Wheels (Chrome, Dirty)",
    "truck wheel clean": "Al Truck Wheels",
    "al wire bare catv": "CATV Al Wire (Bare)",
    "al wire neoprene & acsr 68% prepared": "Insulated Al Wire (ACSR 68%)",
    "al wire ec wire prepared": "EC Aluminum Wire",
    "al mlc mix low copper sheet/plate/clip solid prepared": "MLC (Mixed Low Copper) Prepared",

    # ---- Auto electrics / coils ----
    "auto coil-al starter al nose": "Auto Starter (Al Nose)",
    "auto coil-truck starter fe nose, truck starter": "Truck Starter (Fe Nose)",
    "auto coil-alternator no bus alternator": "Alternator (No Bus)",
    "auto coil-auto compressor no steel case & hose": "Auto A/C Compressor (No Steel Case/Hose)",

    # ---- ICW ----
    "icw low grade christmas lights": "ICW Low Grade (Christmas Lights)",
    "icw low grade computer wire 25%": "ICW Low Grade (Computer Wire 25%)",
    "icw low grade cu catv": "ICW Low Grade (Cu CATV)",
    "icw low grade ext. cords 35% up": "ICW Low Grade (Extension Cords 35%+)",
    "icw low grade mixed wire 40% up": "ICW Low Grade (Mixed Wire 40%+)",
    "icw#1 heliax 57%, open eye/baled": "ICW #1 (Heliax 57%, Baled)",
    "icw#1 mcm 85% hg": "ICW #1 (MCM 85% HG)",
    "icw#1 romex 65%, no weather proof.": "ICW #1 (Romex 65%)",
    "icw#1 thhn80%": "ICW #1 (THHN 80%)",
    "icw#2 50%& cat5, tel wire": "ICW #2 (50% & CAT5/Tel)",
    "icw#2 harness wire no fuse boxes": "ICW #2 (Harness, No Fuse Boxes)",
    "icw#2-bx cable al": "ICW #2 BX (Al)",
    "icw#2-bx cable fe, 24%": "ICW #2 BX (Fe, 24%)",

    # ---- Lead (shop terms) ----
    "lead clean clean": "Lead (Soft/Clean)",
    "lead range range lead indoor": "Range Lead (Indoor)",
    "lead wheel weight wheel weight": "Lead Wheel Weights",

    # ---- Tool/Alloy steels ----
    "manganese steel": "Manganese Steel",
    "h-13": "Tool Steel H-13",

    # ---- E-scrap: adapters / laptop note ----
    "e scrap-ac adaptors laptop no cable no wall plugs & no phone chargers": "AC Adapters (Laptop, No Cables/Plugs)",
    "laptop with cable no wall plugs & phone chargers": "Laptops (With Cable, No Wall Plugs/Chargers)",

    # ---- E-scrap: batteries ----
    "e scrap-battery modem/laptop battery": "Laptop/Modem Batteries",
    "cell phone battery - lithium ion": "Cell Phone Battery (Li-ion)",

    # ---- E-scrap: others ----
    "e scrap-others plastic computer fan": "Plastic Computer Fan",
    "cable box with hd": "Cable Box (With HDD)",
    "cable box no hd w green board": "Cable Box (No HDD, Green Board)",
    "cell phone no battery": "Cell Phone (No Battery)",
    "cell phone with battery": "Cell Phone (With Battery)",
    "circuit breakers": "Circuit Breakers",
    "cpu gold": "CPU (Gold)",
    "credit card pos reader - stationary": "POS Reader (Stationary)",
    "credit card pos reader -mobile": "POS Reader (Mobile)",
    "docking stations": "Docking Station",
    "ecm box, no jelly": "ECM Box (No Potting)",
    "fuse box from car": "Automotive Fuse Box",
    "lcd monitor broken": "LCD Monitor (Broken)",
    "network equipment, switch": "Network Switch",
    "networking metal": "Networking Metal Scrap",
    "ac unit whole - window mount": "AC Unit (Window, Whole)",
    "printer / copiers - palletized/shrink wrapped": "Printers/Copiers (Palletized)",
    "router - steel": "Router (Steel)",
    "routers-plastic modem": "Router/Modem (Plastic)",
    "e meter w glass cover": "Electric Meter (Glass Cover)",
    "e meter - digital": "Electric Meter (Digital)",
    "e meter - analog,no glass": "Electric Meter (Analog, No Glass)",
    "telephone office": "Telephone Office Gear",
    "ups battery backup": "UPS Battery Backup",
    "plug end breakage mix": "Plug Ends (Breakage Mix)",
    "alu computer fan": "Aluminum Computer Fan",
    "printer / copiers - unprepared": "Printers/Copiers (Unprepared)",
    "ribbon cable": "Ribbon Cable",

    # ---- E-scrap: computers/laptops/drives ----
    "e scrap-computer scrap complete computer tower": "Computer Tower (Complete)",
    "incomplete tower w/o ram, cup, hard drive": "Computer Tower (Incomplete, No RAM/CPU/HDD)",
    "mix computer tower": "Computer Towers (Mixed)",
    "e scrap-laptop complete w good screen 15\"+": "Laptop (Complete, ≥15\" Good Screen)",
    "incomplete 12\"+": "Laptop (Incomplete, ≥12\")",
    "chrome book/tablet w good screen": "Chromebook/Tablet (Good Screen)",
    "e scrap-cd rom cd rom": "CD-ROM Drives",
    "e scrap-hard drive hard drive no board": "Hard Drive (No Board)",
    "hard drive no board shredded": "Hard Drive (No Board, Shredded)",
    "hard drive with full board": "Hard Drive (Full Board)",
    "hard drive with partial board": "Hard Drive (Partial Board)",
    "hard drive with board -punched / bent / cracked or with caddy": "Hard Drive (Board, Damaged/Caddy)",
    "hard drive with board shredded": "Hard Drive (Board, Shredded)",

    # ---- Power supplies / servers / boards ----
    "e scrap-power supply server power supply": "Server Power Supply",
    "with cable": "Power Supply (With Cable)",
    "without cable": "Power Supply (Without Cable)",
    "e scrap-server blade server": "Blade Server",
    "complete": "Server (Complete)",
    "incomplete": "Server (Incomplete)",
    "server cabinet": "Server Cabinet",
    "e scrap-circuit board auto shredder board": "Auto Shredder Board",
    "green motherboard": "Motherboard (Green)",
    "color motherboard": "Motherboard (Mixed Colors)",
    "crt board": "CRT Board",
    "finger board clean no attachment": "Finger Cards (Clean, No Attachments)",
    "gold ram / no bracket": "RAM (Gold Fingers, No Bracket)",
    "silver ram": "RAM (Silver)",
    "medium grade board": "Circuit Board (Medium Grade)",
    "power board green": "Power Board (Green)",
    "power board brown": "Power Board (Brown)",
    "server motherboard": "Server Motherboard",
    "telecom board high grade": "Telecom Board (High Grade)",

    # ---- Stainless ----
    "ss 304 ss 304 prepared": "Stainless 304 (Prepared)",
    "ss 316 solid clean prepared": "Stainless 316 (Clean, Prepared)",

    # ---- Ballasts / Batteries ----
    "ballast electronic ballast": "Ballasts (Electronic)",
    "ballast regular": "Ballasts (Magnetic/Regular)",
    "battery auto pb-acid battery": "Lead-Acid Batteries (Auto)",
    "battery steel case battery steel case industrial": "Lead-Acid Batteries (Steel Case, Industrial)",

    # ---- Motors / Transformers ----
    "compressor sealed unit boxed, no cast iron case": "Sealed Units (Compressors, No Cast Case)",
    "e motor low grade celling fan motors": "Electric Motors (Low Grade/Ceiling Fans)",
    "e motor mix no pumps, power tools, ceiling fans.": "Electric Motors (Mixed, No Pumps/Power Tools/Ceiling Fans)",
    "e motor large big, but less than 1000 pounds": "Electric Motors (Large <1000 lb)",
    "transformer transformer alu large only": "Transformers (Aluminum, Large Only)",
    "transformer cu al/cu": "Transformers (Al/Cu)",
    "transformer cu cu small (palm size)": "Transformers (Copper, Small)",
    "transformer cu medium (more than 200lbs)": "Transformers (Copper, Medium)",
    "transformer cu large": "Transformers (Copper, Large)",

    # ---- Zinc quick ----
    "zinc zinc/die cast": "Zinc Die Cast",

    # ---- Brass shop terms ----
    "brass hard solid clean": "Brass (Hard, Clean)",
    "brass red ebony": "Red Brass (Ebony)",
    "brass red semi red": "Red Brass (Semi)",
    "brass shaving al bronze shaving, c/d": "Al Bronze Shavings (C/D)",
    "brass shaving brass red shaving, c/d": "Red Brass Shavings (C/D)",
    "brass shaving brass yellow shaving, c/d": "Yellow Brass Shavings (C/D)",
    "brass shell clean no chrome": "Brass Shells (Clean, No Chrome)",
    "brass special al bronze solid": "Al Bronze (Solid)",
    "brass yellow regular solid clean": "Yellow Brass (Solid, Clean)",

    # ---- Copper quick terms ----
    "cu bb barley": "No. 1 Copper Wire (Barley)",
    "cu#1 berry & candy": "No. 1 Copper (Berry/Candy)",
    "cu#2 birch & cliff": "No. 2 Copper (Birch/Cliff)",
    "cu#3 sheet copper": "Sheet Copper",

    # ---- Radiators / A/C ----
    "a/c reefer clean talk bailed": "A/C Radiators (Clean, Baled)",
    "a/c reefer clean unbaled": "A/C Radiators (Clean, Unbaled)",
    "a/c reefer dirty talk bailed": "A/C Radiators (Dirty, Baled)",
    "a/c reefer dirty unbaled": "A/C Radiators (Dirty, Unbaled)",
    "radiator clean, car radiator": "Car Radiators (Clean)",
    "cu bkg ac reefer end": "Copper Breakage (A/C Reefer Ends)",
}
# ---- Material normalization ---- 

# ----- Keyword rules (applied after exact map) ----
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

KEYWORD_RULES += [
    (re.compile(r"\bold\s*sheet\b", re.I), "Old Sheet"),
    (re.compile(r"\bubc|alum\s*cans?\b", re.I), "Alum Cans"),
]
#---- Keyword rules (applied after exact map) -----

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
    for _ in range(240):  # ~4 minutes
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
    base = _bridge_base_for_doc()
    r = session.post(f"{base}/login",
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

        # Back-date created_at for historical imports using the QBO invoice date
        inv_date = row.get("invoice_date")
        if headers.get("X-Import-Mode") == "historical" and inv_date:
            if isinstance(inv_date, str):
                # 'YYYY-MM-DD' → add midnight UTC
                if len(inv_date) == 10 and inv_date[4] == "-" and inv_date[7] == "-":
                    headers["X-Import-Created-At"] = inv_date + "T00:00:00Z"
                else:
                    # already ISO-ish; backend will normalize
                    headers["X-Import-Created-At"] = inv_date
            else:
                try:
                    from datetime import datetime, timezone
                    if isinstance(inv_date, datetime):
                        headers["X-Import-Created-At"] = inv_date.astimezone(timezone.utc).isoformat()
                except Exception:
                    pass

    if ENV in {"ci", "test"} or HARVESTER_DISABLED:
        print("[bridge] Skipped (CI/test mode)")
        return {"ok": True, "stub": True}

    base = _bridge_base_for_doc("invoice")
    url  = f"{base.rstrip('/')}/contracts"
    r = session.post(url, json=payload, timeout=25, headers=headers)
    if r.status_code not in (200, 201):
        print(f"[bridge] POST /contracts failed [{r.status_code}] {r.text[:200]}")
    return r

def _row_to_bridge_contract(row: dict, seller_name: str = "Winski Brothers") -> dict | None:
    """
    Translate a harvested QBO line-item `row` into the exact payload BRidge /contracts expects.
    Returns None if qty/unit_price missing.
    """
    qty = row.get("qty")
    unit_price = row.get("unit_price")
    if qty is None or unit_price is None:
        return None

    uom = (row.get("uom") or "").lower()
    if uom in ("lb", "lbs", "pound", "pounds"):
        weight_tons = float(qty) / 2000.0
        price_per_ton = float(unit_price) * 2000.0
    elif uom in ("ton", "tons", "t"):
        weight_tons = float(qty)
        price_per_ton = float(unit_price)
    else:
        # Fallback assume pounds
        weight_tons = float(qty) / 2000.0
        price_per_ton = float(unit_price) * 2000.0

    material_canon = normalize_material(
        row.get("item_original") or row.get("item") or "", row.get("customer")
    )

    return {
        "buyer": row.get("customer"),
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
    }

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
    if RELAY_BASE and RELAY_AUTH and REDIRECT_URI and REDIRECT_URI.startswith("https://"):
        return oauth_flow_via_relay()
    return oauth_flow()

def main():
    toks = get_tokens()

    # Dump all customers (helps pick exact names later)
    toks = dump_customers_csv(toks)

    rows = []
    sess = requests.Session()
    try:
        bridge_login(sess)
    except Exception as e:
        print(f"[bridge] login skipped/failed: {e}")

    matched_any = False

    # ---- Per-customer loop (unchanged) ----
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

                item_name   = (d.get("ItemRef") or {}).get("name")
                line_desc   = L.get("Description")  # <-- often the true material text
                svc_date    = d.get("ServiceDate")  # optional; fallback to invoice date later
                qty         = d.get("Qty")
                unitprice   = d.get("UnitPrice")
                amount      = L.get("Amount")
                uom         = d.get("UnitOfMeasure")
                # Prefer Description for material normalization; fallback to Item name
                source_for_material = line_desc or item_name or ""
                material_canon = normalize_material(
                    source_for_material,
                    customer_name=(inv.get("CustomerRef") or {}).get("name")
                )

                row = {
                    "customer": (inv.get("CustomerRef") or {}).get("name") or name if 'name' in locals() else (inv.get("CustomerRef") or {}).get("name"),
                    "invoice_id": inv_id,
                    "invoice_number": doc_no,
                    "invoice_date": inv_date,
                    "service_date": svc_date or inv_date,            # Not always present
                    "product_service": item_name,                    # QBO item
                    "description": line_desc,                        # “Shred” text
                    "ship_date": ship_dt,
                    "ship_via": ship_m,
                    "item": material_canon,
                    "item_original": item_name,
                    "qty": qty,
                    "uom": uom,
                    "unit_price": unitprice,                         # aka Rate
                    "line_amount": amount,                           # Amount
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

    # ---- Fallback: now correctly OUTSIDE the per-customer loop ----
    if not matched_any:
        print("[fallback] No CUSTOMER_NAMES matched. Pulling ALL invoices in date window…")
        invs, toks = get_invoices_all(toks, START.isoformat(), END.isoformat())
        print(f"[ALL CUSTOMERS] {len(invs)} invoices")

        cdir = PDFS_DIR / "_ALL"
        cdir.mkdir(parents=True, exist_ok=True)

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

            # line items for this invoice (fallback path)
            for idx, L in enumerate(inv.get("Line", [])):
                if L.get("DetailType") != "SalesItemLineDetail":
                    continue
                d = L.get("SalesItemLineDetail", {})

                item_name = (d.get("ItemRef") or {}).get("name")
                line_desc = L.get("Description")
                svc_date  = d.get("ServiceDate")
                qty       = d.get("Qty")
                unitprice = d.get("UnitPrice")
                amount    = L.get("Amount")
                uom       = d.get("UnitOfMeasure")

                source_for_material = line_desc or item_name or ""
                material_canon = normalize_material(
                    source_for_material,
                    customer_name=custname
                )

                row = {
                    "customer": custname,
                    "invoice_id": inv_id,
                    "invoice_number": doc_no,
                    "invoice_date": inv_date,
                    "service_date": svc_date or inv_date,
                    "product_service": item_name,
                    "description": line_desc,
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
    # ---- Build BRidge-ready contracts from harvested rows ----
    contracts = []
    for r in rows:
        c = _row_to_bridge_contract(r, seller_name=BRIDGE_SELLER)
        if c:
            contracts.append(c)

    # Write contracts CSV (exact schema BRidge expects)
    contract_headers = [
        "buyer","seller","material","weight_tons","price_per_ton",
        "pricing_formula","reference_symbol","reference_price",
        "reference_source","reference_timestamp","currency"
    ]
    with open(CONTRACTS_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=contract_headers)
        w.writeheader()
        w.writerows(contracts)

    # Also write NDJSON (for bulk import / debugging)
    with open(CONTRACTS_JSONL_PATH, "w", encoding="utf-8") as f:
        for c in contracts:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")

    # ---- Final CSV write (base indent of main) ----
    headers = [
        "customer","invoice_id","invoice_number","invoice_date","service_date",
        "product_service","description",
        "ship_date","ship_via",
        "item","item_original","qty","uom","unit_price","line_amount","invoice_total",
        "invoice_balance","pdf_path"
    ]

    # Drop internal field before writing
    for r in rows:
        r.pop("_line_index", None)

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)

    print(f"Done → {CSV_PATH}  | PDFs under {PDFS_DIR}")

if __name__ == "__main__":
    main()
