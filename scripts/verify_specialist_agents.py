from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from apps.pricing_stock_release_agent.worker import process_work_item as process_pricing
from apps.sales_income_agent.worker import process_work_item as process_sales
from packages.signal_contracts.work_item import WorkItem

SALES_SAMPLE = """TTC 5th Street branch
Date: Monday 06/04/2026

DAY-END SALES REPORT
Till#1: Main Shop
Cashier: Imelda Patrick
Assistant: Joycelyn

T/Cash:K2,741.00
T/Card: K2,991.00
Z/Reading: K5,732.00

TOTALS
Total Cash: K2,741.00
Total Card: K2,991.00
Total Sales: K5,732.00

CUSTOMER COUNT
Main Door: 259
Guest/customer serve: 113
"""

PRICING_SAMPLE = """Branch: TTC POM Waigani Branch
Date: 06/04/26
Prepared By: Maria Sine (Supervisor)

#01.OSH
(Qty:229)
Amt:K2,089.00

#02.Toys
(Qty:46)
Amt:K392.00

#03.Tshirts
(Qty:153)
Amt:K801.00

Total bales break today Five(05)
Three(03) released
Two(02) pending approval

Total quantity:428pcs
Total Amount: K3,281.00
"""


def run_sales_test() -> None:
    print("\n=== SALES INCOME AGENT ===")

    item = WorkItem(
        kind="raw_message",
        payload={
            "classification": {"report_type": "sales"},
            "raw_message": {"text": SALES_SAMPLE},
        },
    )

    result = process_sales(item)
    payload = result.payload

    print(json.dumps(payload, indent=2))

    assert payload["signal_type"] == "sales_income"
    assert payload["source_agent"] == "sales_income_agent"
    assert payload["report_date"] == "2026-04-06"
    assert payload["status"] == "ready"
    assert payload["warnings"] == []
    assert payload["metrics"]["cash_sales"] == 2741.0
    assert payload["metrics"]["eftpos_sales"] == 2991.0
    assert payload["metrics"]["gross_sales"] == 5732.0
    assert payload["metrics"]["traffic"] == 259
    assert payload["metrics"]["served"] == 113

    print("PASS sales_income_agent")


def run_pricing_test() -> None:
    print("\n=== PRICING STOCK RELEASE AGENT ===")

    item = WorkItem(
        kind="raw_message",
        payload={
            "classification": {"report_type": "bale_summary"},
            "raw_message": {"text": PRICING_SAMPLE},
        },
    )

    result = process_pricing(item)
    payload = result.payload

    print(json.dumps(payload, indent=2))

    warning_codes = {warning["code"] for warning in payload["warnings"]}

    assert payload["signal_type"] == "pricing_stock_release"
    assert payload["source_agent"] == "pricing_stock_release_agent"
    assert payload["branch"] == "TTC POM Waigani Branch"
    assert payload["report_date"] == "2026-04-06"
    assert payload["status"] == "needs_review"
    assert payload["metrics"]["bales_processed"] == 5
    assert payload["metrics"]["bales_released"] == 3
    assert payload["metrics"]["bales_pending_approval"] == 2
    assert payload["metrics"]["total_qty"] == 428
    assert payload["metrics"]["total_amount"] == 3282.0
    assert payload["metrics"]["release_ratio"] == 0.6
    assert payload["provenance"] == {
        "prepared_by": "Maria Sine",
        "role": "Supervisor",
    }
    assert len(payload["items"]) == 3
    assert warning_codes == {"data_mismatch", "approval_backlog", "low_release_ratio"}

    print("PASS pricing_stock_release_agent")


def main() -> None:
    checks = [
        ("sales_income_agent", run_sales_test),
        ("pricing_stock_release_agent", run_pricing_test),
    ]
    passed: list[str] = []
    failed: list[str] = []

    for name, check in checks:
        try:
            check()
        except AssertionError as exc:
            failed.append(name)
            print(f"FAIL {name}: {exc}")
        except Exception as exc:
            failed.append(name)
            print(f"FAIL {name}: {exc.__class__.__name__}: {exc}")
        else:
            passed.append(name)

    print("\n=== FINAL SUMMARY ===")
    print(f"passed: {len(passed)}")
    for name in passed:
        print(f"- {name}")
    print(f"failed: {len(failed)}")
    for name in failed:
        print(f"- {name}")

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
