"""
enrich_monographs.py
--------------------
Links your existing Egyptian drugs SQLite database to Drowsyng clinical
monograph data via active_ingredient → generic_name fuzzy matching.

Usage:
    python enrich_monographs.py \
        --db      output/egyptian_pharma.sqlite \
        --csv     data/drowsyng.csv \
        --threshold 80

Output:
    Adds a `monographs` table to your existing SQLite DB.
    Each row is linked to drugs.id via drug_id (foreign key).
"""

import argparse
import csv
import re
import sqlite3
import sys
from pathlib import Path

from rapidfuzz import fuzz, process

# ---------------------------------------------------------------------------
# Section parser
# ---------------------------------------------------------------------------

# Common headings found in Drowsyng drug_content blobs.
# Order matters: earlier patterns shadow later ones during splitting.
SECTION_PATTERNS = [
    ("uses",            r"uses?|indications?|used\s+for|what\s+is\s+.+used\s+for"),
    ("mechanism",       r"how\s+.+works?|mechanism\s+of\s+action|pharmacology"),
    ("dosage",          r"dosage|dose|how\s+to\s+use|directions?|administration"),
    ("warnings",        r"warnings?|precautions?|before\s+you\s+use|caution"),
    ("side_effects",    r"side[\s\-]?effects?|adverse\s+(effects?|reactions?)"),
    ("interactions",    r"interactions?|drug\s+interactions?"),
    ("contraindications", r"contraindications?|do\s+not\s+use|when\s+not\s+to\s+use"),
    ("storage",         r"storage|store|keep"),
]

# Build one compiled regex that detects any heading line
_HEADING_RE = re.compile(
    r"^\s*(?:" + "|".join(p for _, p in SECTION_PATTERNS) + r")\s*[:\-]?\s*$",
    re.IGNORECASE | re.MULTILINE,
)

def _label_for(heading_text: str) -> str:
    h = heading_text.strip().lower()
    for label, pattern in SECTION_PATTERNS:
        if re.search(pattern, h, re.IGNORECASE):
            return label
    return "other"


def parse_sections(content: str) -> dict[str, str]:
    """
    Split a free-text drug_content blob into labelled sections.
    Returns a dict like:
        {"uses": "...", "warnings": "...", "side_effects": "...", ...}
    Falls back to storing the whole blob under "uses" if no headings found.
    """
    if not content:
        return {}

    # Try to split on heading lines
    parts = re.split(r"\n(?=\s*[A-Z][^\n]{0,60}[:\-]?\s*\n)", content)
    if len(parts) <= 1:
        # No clear heading breaks — store entire content as uses
        return {"uses": content.strip()}

    sections: dict[str, str] = {}
    current_label = "uses"
    current_lines: list[str] = []

    for line in content.splitlines():
        if _HEADING_RE.match(line):
            if current_lines:
                text = "\n".join(current_lines).strip()
                if text:
                    sections[current_label] = text
            current_label = _label_for(line)
            current_lines = []
        else:
            current_lines.append(line)

    if current_lines:
        text = "\n".join(current_lines).strip()
        if text:
            sections[current_label] = text

    # If parsing produced nothing useful, store whole blob
    if not sections:
        sections["uses"] = content.strip()

    return sections


# ---------------------------------------------------------------------------
# Normalizer
# ---------------------------------------------------------------------------

_STRIP_RE = re.compile(
    r"\b(hydrochloride|hcl|trihydrate|monohydrate|sodium|potassium|calcium"
    r"|sulfate|sulphate|phosphate|maleate|tartrate|acetate|nitrate|citrate"
    r"|mg|mcg|g|ml|%|iu|tablet|capsule|injection|solution|cream|gel|drops?)\b",
    re.IGNORECASE,
)
_WS_RE = re.compile(r"\s+")


def normalize_ingredient(text: str) -> str:
    """Lowercase, strip salt suffixes and units, collapse whitespace."""
    if not text:
        return ""
    t = text.lower()
    t = _STRIP_RE.sub(" ", t)
    t = re.sub(r"[^\w\s\+\-]", " ", t)
    t = _WS_RE.sub(" ", t).strip()
    return t


# ---------------------------------------------------------------------------
# CSV loader
# ---------------------------------------------------------------------------

# Flexible column aliases for the Drowsyng CSV
_COL_ALIASES = {
    "generic_name":  ["generic_name", "generic", "active_ingredient", "active ingredient"],
    "med_name":      ["med_name", "medicine_name", "drug_name", "name", "brand_name"],
    "drug_content":  ["drug_content", "content", "description", "monograph", "details"],
    "disease_name":  ["disease_name", "disease", "indication", "condition"],
}


def _resolve_col(header: list[str], aliases: list[str]) -> str | None:
    h_lower = [c.lower().strip() for c in header]
    for alias in aliases:
        if alias in h_lower:
            return header[h_lower.index(alias)]
    return None


def load_drowsyng(csv_path: str) -> list[dict]:
    """
    Load Drowsyng CSV rows, resolving flexible column names.
    Returns list of dicts with keys: generic_name, med_name, drug_content, disease_name.
    Skips rows where both generic_name and med_name are empty.
    """
    rows = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []

        col = {
            field: _resolve_col(list(header), aliases)
            for field, aliases in _COL_ALIASES.items()
        }

        missing = [f for f, c in col.items() if c is None and f in ("generic_name", "drug_content")]
        if missing:
            print(f"[WARN] Could not find columns for: {missing}")
            print(f"       Available columns: {header}")

        for raw in reader:
            generic  = (raw.get(col["generic_name"])  or "").strip() if col["generic_name"]  else ""
            med      = (raw.get(col["med_name"])       or "").strip() if col["med_name"]       else ""
            content  = (raw.get(col["drug_content"])   or "").strip() if col["drug_content"]   else ""
            disease  = (raw.get(col["disease_name"])   or "").strip() if col["disease_name"]   else ""

            if not generic and not med:
                continue
            if not content:
                continue

            rows.append({
                "generic_name": generic,
                "med_name":     med,
                "drug_content": content,
                "disease_name": disease,
                "norm_generic": normalize_ingredient(generic or med),
            })

    print(f"[INFO] Loaded {len(rows)} Drowsyng rows with content")
    return rows


# ---------------------------------------------------------------------------
# Fuzzy matcher
# ---------------------------------------------------------------------------

def build_index(drowsyng_rows: list[dict]) -> dict[str, list[dict]]:
    """
    Group Drowsyng rows by normalized generic_name.
    One generic can have multiple rows (different disease contexts).
    """
    index: dict[str, list[dict]] = {}
    for row in drowsyng_rows:
        key = row["norm_generic"]
        if key:
            index.setdefault(key, []).append(row)
    return index


def find_best_match(
    norm_ingredient: str,
    index: dict[str, list[dict]],
    threshold: int,
) -> list[dict] | None:
    """
    Return Drowsyng rows for the best-matching generic_name, or None.
    Uses token_sort_ratio for salt-stripped, reordered names.
    """
    if not norm_ingredient or not index:
        return None

    keys = list(index.keys())
    result = process.extractOne(
        norm_ingredient,
        keys,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=threshold,
    )
    if result:
        matched_key, score, _ = result
        return index[matched_key]
    return None


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS monographs (
    id          INTEGER PRIMARY KEY,
    drug_id     INTEGER NOT NULL REFERENCES drugs(id) ON DELETE CASCADE,
    section     TEXT    NOT NULL,   -- 'uses' | 'mechanism' | 'dosage' | 'warnings' |
                                    -- 'side_effects' | 'interactions' | 'contraindications' |
                                    -- 'storage' | 'other'
    content     TEXT    NOT NULL,
    language    TEXT    NOT NULL DEFAULT 'en',
    source      TEXT    DEFAULT 'Drowsyng Dataset',
    UNIQUE(drug_id, section, language)
);

CREATE INDEX IF NOT EXISTS idx_monograph_drug   ON monographs(drug_id);
CREATE INDEX IF NOT EXISTS idx_monograph_section ON monographs(section);

-- FTS5 for full-text search across monograph content
CREATE VIRTUAL TABLE IF NOT EXISTS monographs_fts USING fts5(
    content,
    section UNINDEXED,
    language UNINDEXED,
    content='monographs',
    content_rowid='id',
    tokenize='unicode61 remove_diacritics 1'
);

-- Keep FTS in sync
CREATE TRIGGER IF NOT EXISTS monographs_ai AFTER INSERT ON monographs BEGIN
    INSERT INTO monographs_fts(rowid, content, section, language)
    VALUES (new.id, new.content, new.section, new.language);
END;
CREATE TRIGGER IF NOT EXISTS monographs_ad AFTER DELETE ON monographs BEGIN
    INSERT INTO monographs_fts(monographs_fts, rowid, content, section, language)
    VALUES ('delete', old.id, old.content, old.section, old.language);
END;
CREATE TRIGGER IF NOT EXISTS monographs_au AFTER UPDATE ON monographs BEGIN
    INSERT INTO monographs_fts(monographs_fts, rowid, content, section, language)
    VALUES ('delete', old.id, old.content, old.section, old.language);
    INSERT INTO monographs_fts(rowid, content, section, language)
    VALUES (new.id, new.content, new.section, new.language);
END;
"""


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def enrich(db_path: str, csv_path: str, threshold: int) -> None:
    drowsyng = load_drowsyng(csv_path)
    index    = build_index(drowsyng)

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON")
    con.executescript(SCHEMA_SQL)

    # Load drugs that have an active_ingredient and no monograph yet
    cur = con.execute("""
        SELECT d.id, d.trade_name, d.active_ingredient
        FROM drugs d
        WHERE d.active_ingredient IS NOT NULL
          AND d.active_ingredient != ''
          AND NOT EXISTS (
              SELECT 1 FROM monographs m WHERE m.drug_id = d.id
          )
    """)
    drugs = cur.fetchall()
    print(f"[INFO] {len(drugs)} drugs to enrich")

    matched = skipped = inserted = 0

    for drug_id, trade_name, active_ingredient in drugs:
        norm = normalize_ingredient(active_ingredient)
        candidates = find_best_match(norm, index, threshold)

        if not candidates:
            skipped += 1
            continue

        matched += 1
        # Merge content from all matched rows (different disease contexts)
        merged_content = "\n\n".join(
            r["drug_content"] for r in candidates if r["drug_content"]
        )
        sections = parse_sections(merged_content)

        for section_name, section_text in sections.items():
            try:
                con.execute("""
                    INSERT OR IGNORE INTO monographs
                        (drug_id, section, content, language, source)
                    VALUES (?, ?, ?, 'en', 'Drowsyng Dataset')
                """, (drug_id, section_name, section_text))
                inserted += 1
            except sqlite3.IntegrityError:
                pass

    con.commit()
    con.close()

    print(f"[INFO] Done — matched: {matched} | skipped: {skipped} | sections inserted: {inserted}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Enrich Egyptian drugs SQLite DB with Drowsyng clinical monographs"
    )
    parser.add_argument("--db",        required=True, help="Path to your SQLite database")
    parser.add_argument("--csv",       required=True, help="Path to Drowsyng CSV file")
    parser.add_argument("--threshold", type=int, default=80,
                        help="Fuzzy match score cutoff 0–100 (default: 80)")
    args = parser.parse_args()

    if not Path(args.db).exists():
        print(f"[ERROR] DB not found: {args.db}", file=sys.stderr)
        sys.exit(1)
    if not Path(args.csv).exists():
        print(f"[ERROR] CSV not found: {args.csv}", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] DB:        {args.db}")
    print(f"[INFO] CSV:       {args.csv}")
    print(f"[INFO] Threshold: {args.threshold}")
    enrich(args.db, args.csv, args.threshold)


if __name__ == "__main__":
    main()