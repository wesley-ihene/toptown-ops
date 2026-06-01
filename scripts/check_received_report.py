#!/usr/bin/env python3
from pathlib import Path
import json
import sys

ROOT = Path.cwd()

BRANCH = "waigani"
DATE = "2026-05-20"
REPORT_TYPE = "sales_income"

CANDIDATES = [
    ROOT / "records" / "structured" / REPORT_TYPE / BRANCH / f"{DATE}.json",
    ROOT / "records" / "review" / DATE.replace("-", "_") / BRANCH / REPORT_TYPE,
    ROOT / "records" / "responses" / "whatsapp" / DATE,
]

def load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

print("Checking TAOP report receipt")
print(f"Branch: {BRANCH}")
print(f"Date: {DATE}")
print("Type: Day-End Sales Report / sales_income")
print("-" * 60)

found = False

structured = CANDIDATES[0]
if structured.exists():
    found = True
    data = load_json(structured)
    print(f"✅ STRUCTURED RECORD FOUND: {structured}")
    if isinstance(data, dict):
        print(f"branch: {data.get('branch')}")
        print(f"date: {data.get('date') or data.get('report_date')}")
        print(f"record_type: {data.get('record_type') or data.get('report_type')}")
else:
    print(f"❌ No structured record at: {structured}")

review_dir = CANDIDATES[1]
if review_dir.exists():
    found = True
    print(f"⚠️ REVIEW RECORDS FOUND: {review_dir}")
    for p in sorted(review_dir.glob("*.json"))[-5:]:
        print(f" - {p}")

responses_dir = CANDIDATES[2]
if responses_dir.exists():
    duplicate_hits = []
    for p in responses_dir.glob("*.json"):
        data = load_json(p)
        text = json.dumps(data, default=str).lower() if data else ""
        if "duplicate" in text and "waigani" in text and ("sales" in text or "day-end" in text):
            duplicate_hits.append(p)

    if duplicate_hits:
        found = True
        print("⚠️ DUPLICATE RESPONSE FOUND:")
        for p in duplicate_hits[-5:]:
            print(f" - {p}")

print("-" * 60)

if found:
    print("RESULT: TAOP has received or processed this report.")
    sys.exit(0)

print("RESULT: No matching structured/review/duplicate evidence found.")
sys.exit(1)
