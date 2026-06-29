"""
enrich_monographs.py  (v3 — fixes section parsing for no-newline Drowsyng blobs)
---------------------------------------------------------------------------------
python enrich_monographs.py --db drugged.db --csv drowsyng.csv --threshold 72
"""

import argparse, csv, re, sqlite3, sys
from pathlib import Path
from rapidfuzz import fuzz, process

# ── Normalizer ───────────────────────────────────────────────────────────────

_PREFIX_RE = re.compile(r"^generic\s+name\s*", re.IGNORECASE)
_SALT_RE   = re.compile(
    r"\b(hydrochloride|hcl|trihydrate|monohydrate|sodium|potassium|calcium"
    r"|sulfate|sulphate|phosphate|maleate|tartrate|acetate|nitrate|citrate"
    r"|bisglycinate|gluconate|fumarate|succinate|bromide|chloride|iodide"
    r"|benzoate|stearate|oxide|carbonate|hydroxide|lactate|decanoate"
    r"|mg|mcg|ug|g\b|ml|%|iu|i\.u|meq|fip|drops?)\b",
    re.IGNORECASE,
)
_NUM_RE  = re.compile(r"\b\d+(\.\d+)?\b")
_JUNK_RE = re.compile(
    r"\b(tablet|capsule|injection|solution|cream|gel|ointment|syrup|suspension"
    r"|oral|topical|serving|ingredients?|complex|extract|traditional|active"
    r"|mkt|country|origin|strip|vit\b|vitamin\b)\b",
    re.IGNORECASE,
)

def normalize(text: str) -> str:
    if not text: return ""
    t = text.lower()
    t = _PREFIX_RE.sub("", t)
    t = _SALT_RE.sub(" ", t)
    t = _NUM_RE.sub(" ", t)
    t = _JUNK_RE.sub(" ", t)
    t = re.sub(r"[^\w\s\+]", " ", t)
    return re.sub(r"\s+", " ", t).strip()

_JUNK_NORM_RE = re.compile(r"^[\d\s\-\.]+$")

def is_dirty(raw: str, norm: str) -> bool:
    if not raw or not norm: return True
    if _JUNK_NORM_RE.match(norm): return True
    if len(norm) < 4: return True
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw.strip()): return True
    return False

# ── Section parser (handles Drowsyng's no-newline blobs) ────────────────────
#
# Drowsyng content is one continuous string like:
#   "INTRODUCTION ABOUT DRUGXDRUGX contains ... USES OF DRUGXDrugx is used..."
# We split on the ALL-CAPS heading boundaries.

_HEADING_SPLIT = re.compile(
    r"((?:INTRODUCTION ABOUT|USES OF|HOW .{1,50} WORKS?|"
    r"WARNINGS? AND PRECAUTIONS?|SIDE EFFECTS? OF|"
    r"DRUG INTERACTIONS? OF|CONTRAINDICATIONS? OF|"
    r"DOSAGE AND DIRECTIONS?|DIRECTIONS? FOR USE|"
    r"HOW TO USE|STORAGE OF|STORAGE AND DISPOSAL)"
    r"[^a-z]{0,50}?)(?=[A-Z][a-z])"
)

_SECTION_MAP = [
    ("uses",              re.compile(r"introduction about|uses of|indications", re.I)),
    ("mechanism",         re.compile(r"how .+ works?|mechanism", re.I)),
    ("dosage",            re.compile(r"dosage|directions? for use|how to use", re.I)),
    ("warnings",          re.compile(r"warnings?|precautions?", re.I)),
    ("side_effects",      re.compile(r"side.?effects?", re.I)),
    ("interactions",      re.compile(r"interactions?", re.I)),
    ("contraindications", re.compile(r"contraindications?", re.I)),
    ("storage",           re.compile(r"storage", re.I)),
]

def label_heading(heading: str) -> str:
    for label, pat in _SECTION_MAP:
        if pat.search(heading):
            return label
    return "other"

def parse_sections(content: str) -> dict[str, str]:
    if not content:
        return {}

    parts = _HEADING_SPLIT.split(content)
    # parts alternates: [pre, heading, body, heading, body, ...]
    sections: dict[str, str] = {}

    if len(parts) <= 1:
        # No headings found — store whole thing as uses
        return {"uses": content.strip()}

    # First chunk before any heading goes to uses
    if parts[0].strip():
        sections["uses"] = parts[0].strip()

    i = 1
    while i < len(parts) - 1:
        heading = parts[i].strip()
        body    = parts[i + 1].strip() if i + 1 < len(parts) else ""
        label   = label_heading(heading)
        if body:
            sections.setdefault(label, body)
        i += 2

    return sections or {"uses": content.strip()}

# ── CSV loader ───────────────────────────────────────────────────────────────

def load_drowsyng(csv_path: str) -> tuple[list[str], dict[str, list[dict]]]:
    index: dict[str, list[dict]] = {}
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            generic = (row.get("generic_name") or "").strip()
            content = (row.get("drug_content")  or "").strip()
            disease = (row.get("disease_name")  or "").strip()
            if not content: continue
            norm = normalize(generic)
            if not norm or is_dirty(generic, norm): continue
            index.setdefault(norm, []).append({
                "generic_name": generic,
                "drug_content": content,
                "disease_name": disease,
            })
    keys = list(index.keys())
    print(f"[INFO] Drowsyng: {len(keys)} unique normalized generics")
    return keys, index

# ── Matcher ──────────────────────────────────────────────────────────────────

def match(norm_ingredient: str, keys: list[str], threshold: int) -> str | None:
    if not norm_ingredient or not keys: return None
    if norm_ingredient in keys: return norm_ingredient
    parts = [p.strip() for p in norm_ingredient.split("+") if p.strip()]
    candidates = [norm_ingredient] + (parts if len(parts) > 1 else [])
    for candidate in candidates:
        result = process.extractOne(
            candidate, keys,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=threshold,
        )
        if result:
            return result[0]
    return None

# ── Schema ───────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS monographs (
    id      INTEGER PRIMARY KEY,
    drug_id INTEGER NOT NULL REFERENCES drugs(id) ON DELETE CASCADE,
    section TEXT    NOT NULL,
    content TEXT    NOT NULL,
    language TEXT   NOT NULL DEFAULT 'en',
    source  TEXT    DEFAULT 'Drowsyng/Netmeds',
    UNIQUE(drug_id, section, language)
);
CREATE INDEX IF NOT EXISTS idx_mono_drug    ON monographs(drug_id);
CREATE INDEX IF NOT EXISTS idx_mono_section ON monographs(section);
CREATE VIRTUAL TABLE IF NOT EXISTS monographs_fts USING fts5(
    content,
    section UNINDEXED,
    content='monographs',
    content_rowid='id',
    tokenize='unicode61 remove_diacritics 1'
);
CREATE TRIGGER IF NOT EXISTS mono_ai AFTER INSERT ON monographs BEGIN
    INSERT INTO monographs_fts(rowid, content, section)
    VALUES (new.id, new.content, new.section);
END;
CREATE TRIGGER IF NOT EXISTS mono_ad AFTER DELETE ON monographs BEGIN
    INSERT INTO monographs_fts(monographs_fts, rowid, content, section)
    VALUES ('delete', old.id, old.content, old.section);
END;
"""

# ── Pipeline ─────────────────────────────────────────────────────────────────

def enrich(db_path: str, csv_path: str, threshold: int) -> None:
    keys, index = load_drowsyng(csv_path)

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON")
    con.executescript(SCHEMA)

    # Drop existing monographs so we re-insert with fixed section parser
    existing = con.execute("SELECT COUNT(*) FROM monographs").fetchone()[0]
    if existing > 0:
        print(f"[INFO] Dropping {existing} existing monograph rows to re-parse sections...")
        con.execute("DELETE FROM monographs")
        con.execute("DELETE FROM monographs_fts")
        con.commit()

    drugs = con.execute("""
        SELECT id, trade_name, active_ingredient FROM drugs
        WHERE active_ingredient IS NOT NULL
          AND active_ingredient != ''
          AND active_ingredient NOT GLOB '[0-9]*'
    """).fetchall()

    print(f"[INFO] {len(drugs)} drugs to enrich")

    matched = skipped = dirty = inserted = 0

    for drug_id, trade_name, active_ingredient in drugs:
        norm = normalize(active_ingredient)
        if is_dirty(active_ingredient, norm):
            dirty += 1
            continue

        best_key = match(norm, keys, threshold)
        if not best_key:
            skipped += 1
            continue

        matched += 1
        merged = "\n\n".join(r["drug_content"] for r in index[best_key] if r["drug_content"])
        sections = parse_sections(merged)

        for section_name, section_text in sections.items():
            try:
                con.execute("""
                    INSERT OR IGNORE INTO monographs
                        (drug_id, section, content, language, source)
                    VALUES (?, ?, ?, 'en', 'Drowsyng/Netmeds')
                """, (drug_id, section_name, section_text))
                inserted += 1
            except sqlite3.IntegrityError:
                pass

    con.commit()
    con.close()

    avg_sections = round(inserted / matched, 1) if matched else 0
    print(f"\n[RESULT] matched={matched}  skipped={skipped}  dirty={dirty}")
    print(f"         sections inserted={inserted}  avg per drug={avg_sections}")

# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db",        required=True)
    ap.add_argument("--csv",       required=True)
    ap.add_argument("--threshold", type=int, default=72)
    args = ap.parse_args()
    for p, label in [(args.db, "DB"), (args.csv, "CSV")]:
        if not Path(p).exists():
            print(f"[ERROR] {label} not found: {p}", file=sys.stderr); sys.exit(1)
    print(f"[INFO] DB={args.db}  CSV={args.csv}  threshold={args.threshold}")
    enrich(args.db, args.csv, args.threshold)

if __name__ == "__main__":
    main()