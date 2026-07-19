from pathlib import Path
import json

ROOT = Path.home() / ".openclaw" / "workspace" / "toptown-ops"
RESP = ROOT / "records" / "responses" / "whatsapp"

files = sorted(RESP.glob("**/*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:30]

print("TAOP RECENT REJECTION AUDIT")
print("=" * 60)

for f in files:
    try:
        data = json.loads(f.read_text(errors="ignore"))
    except Exception as e:
        print(f"\nFILE: {f.relative_to(ROOT)}")
        print("JSON ERROR:", e)
        continue

    text = json.dumps(data, indent=2, default=str)

    lowered = text.lower()
    if not any(x in lowered for x in ["reject", "failed", "error", "review required"]):
        continue

    print(f"\nFILE: {f.relative_to(ROOT)}")
    print("-" * 60)

    for key in [
        "report",
        "report_type",
        "type",
        "branch",
        "branch_slug",
        "date",
        "status",
        "message",
        "reason",
        "error",
        "failed_checks",
        "issues",
        "validation",
    ]:
        if isinstance(data, dict) and key in data:
            print(f"{key}: {data[key]}")

    print("\nRAW SUMMARY:")
    lines = text.splitlines()
    for line in lines[:80]:
        if any(word in line.lower() for word in ["reject", "failed", "error", "issue", "missing", "invalid", "mismatch", "parser"]):
            print(line.strip())
