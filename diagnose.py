"""
diagnose.py — run this first to see why matching is failing
python diagnose.py --db drugged.db --csv drowsyng.csv
"""
import argparse, csv, re, sqlite3
from rapidfuzz import fuzz, process

_STRIP_RE = re.compile(
    r"\b(hydrochloride|hcl|trihydrate|monohydrate|sodium|potassium|calcium"
    r"|sulfate|sulphate|phosphate|maleate|tartrate|acetate|nitrate|citrate"
    r"|mg|mcg|g|ml|%|iu|tablet|capsule|injection|solution|cream|gel|drops?)\b",
    re.IGNORECASE,
)

def normalize(text):
    if not text: return ""
    t = text.lower()
    t = _STRIP_RE.sub(" ", t)
    t = re.sub(r"[^\w\s\+\-]", " ", t)
    return re.sub(r"\s+", " ", t).strip()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db",  required=True)
    ap.add_argument("--csv", required=True)
    args = ap.parse_args()

    # ── 1. Sample raw values from your DB ──────────────────────────────────
    con = sqlite3.connect(args.db)
    rows = con.execute("""
        SELECT active_ingredient FROM drugs
        WHERE active_ingredient IS NOT NULL AND active_ingredient != ''
        LIMIT 30
    """).fetchall()
    con.close()

    db_samples = [r[0] for r in rows]
    print("=== YOUR DB active_ingredient (raw) ===")
    for s in db_samples:
        print(f"  raw: {repr(s)}")
        print(f"  norm: {repr(normalize(s))}")
    print()

    # ── 2. Sample raw values from Drowsyng CSV ─────────────────────────────
    csv_samples = []
    with open(args.csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        print(f"=== DROWSYNG CSV HEADERS ===\n  {headers}\n")
        for i, row in enumerate(reader):
            if i >= 30: break
            # print all columns for first 3 rows so we can see structure
            if i < 3:
                print(f"--- Row {i} ---")
                for k, v in row.items():
                    print(f"  {k}: {repr((v or '')[:120])}")
                print()
            g = (row.get("generic_name") or row.get("Generic Name") or "").strip()
            csv_samples.append(g)

    print("=== DROWSYNG generic_name (raw + norm) ===")
    for s in csv_samples[:20]:
        print(f"  raw: {repr(s)}")
        print(f"  norm: {repr(normalize(s))}")
    print()

    # ── 3. Try matching a few DB ingredients against CSV ───────────────────
    csv_norms = [normalize(s) for s in csv_samples if s]
    print("=== MATCH ATTEMPTS (threshold=80) ===")
    for db_val in db_samples[:10]:
        norm_db = normalize(db_val)
        result = process.extractOne(norm_db, csv_norms, scorer=fuzz.token_sort_ratio)
        print(f"  DB:    {repr(db_val)}")
        print(f"  norm:  {repr(norm_db)}")
        if result:
            matched, score, _ = result
            print(f"  BEST:  {repr(matched)}  score={score}")
        else:
            print(f"  BEST:  no match")
        print()

if __name__ == "__main__":
    main()