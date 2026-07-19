from pathlib import Path
import json
from collections import Counter, defaultdict

ROOT = Path.home() / ".openclaw" / "workspace" / "toptown-ops"

paths = {
    "raw_whatsapp": ROOT / "records" / "raw" / "whatsapp",
    "responses_whatsapp": ROOT / "records" / "responses" / "whatsapp",
    "normalized": ROOT / "records" / "normalized",
    "signals": ROOT / "records" / "signals",
}

print("TAOP PHASE 1 WHATSAPP INGESTION VERIFICATION")
print("=" * 55)

for name, path in paths.items():
    print(f"{name}: {'FOUND' if path.exists() else 'MISSING'} -> {path}")

print("\nRECENT WHATSAPP RAW FILES")
raw_files = sorted(paths["raw_whatsapp"].glob("**/*.txt"), key=lambda p: p.stat().st_mtime, reverse=True)[:20]
print(f"Count shown: {len(raw_files)}")
for f in raw_files[:10]:
    print("-", f.relative_to(ROOT))

print("\nRECENT WHATSAPP RESPONSE FILES")
response_files = sorted(paths["responses_whatsapp"].glob("**/*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:20]
print(f"Count shown: {len(response_files)}")
for f in response_files[:10]:
    print("-", f.relative_to(ROOT))

print("\nPARSER / VALIDATION SUMMARY")
status_counter = Counter()
report_counter = Counter()
branch_counter = Counter()
date_counter = Counter()
failures = []

for f in response_files:
    try:
        data = json.loads(f.read_text(errors="ignore"))
    except Exception as e:
        failures.append((f, f"JSON read error: {e}"))
        continue

    text = json.dumps(data, default=str).lower()

    if "accepted" in text or "approved" in text or "success" in text or "verified" in text:
        status_counter["accepted_or_verified"] += 1
    if "rejected" in text or "failed" in text or "error" in text:
        status_counter["rejected_or_failed"] += 1

    for key in ["report_type", "report", "type"]:
        val = data.get(key) if isinstance(data, dict) else None
        if val:
            report_counter[str(val)] += 1

    for key in ["branch", "branch_slug"]:
        val = data.get(key) if isinstance(data, dict) else None
        if val:
            branch_counter[str(val)] += 1

    for key in ["date", "report_date"]:
        val = data.get(key) if isinstance(data, dict) else None
        if val:
            date_counter[str(val)] += 1

print("Status:", dict(status_counter))
print("Reports:", dict(report_counter))
print("Branches:", dict(branch_counter))
print("Dates:", dict(date_counter))

print("\nNORMALIZED / SIGNAL FILE CHECK")
for folder_name in ["normalized", "signals"]:
    folder = paths[folder_name]
    if folder.exists():
        files = sorted(folder.glob("**/*"), key=lambda p: p.stat().st_mtime if p.is_file() else 0, reverse=True)
        files = [p for p in files if p.is_file()]
        print(f"{folder_name}: {len(files)} files")
        for f in files[:10]:
            print("-", f.relative_to(ROOT))
    else:
        print(f"{folder_name}: MISSING")

print("\nRESULT")
if raw_files and response_files:
    print("PASS: WhatsApp Test Number is feeding TAOP and responses are being generated.")
else:
    print("FAIL: Raw WhatsApp files or response files are missing.")

if status_counter["rejected_or_failed"]:
    print("WARNING: Some reports are rejected/failed. Review response files above.")
