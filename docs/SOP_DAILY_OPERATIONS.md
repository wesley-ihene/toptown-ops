# TAOP – SOP: Daily Operations

## 1. Report Submission (Stores)

Staff must submit reports via WhatsApp using STRICT format.

### Accepted Report Types:
- DAY-END SALES REPORT
- ATTENDANCE REPORT
- DAILY BALE SUMMARY
- SUPERVISOR CONTROL REPORT

### Rules:
- One report per message
- Correct header (exact spelling)
- Correct branch name
- Correct date
- No mixed reports

---

## 2. System Processing Flow

WhatsApp → Pre-Validation → Orchestrator → Specialist Agent → Governance → Actions

---

## 3. Governance Outcomes (CRITICAL)

Every report will result in ONE of:

### ✅ Accepted
- Structured record created
- Governance sidecar created
- Actions may be generated

### ❌ Rejected (duplicate)
- duplicate_message
- duplicate_semantic
→ No record written

### ❌ Rejected (conflict)
- conflicting_record_same_scope
→ Sent to review queue

### ❌ Rejected (invalid)
- invalid_input
→ Requires correction and resubmission

---

## 4. Supervisor Responsibilities

Supervisors MUST:

- Monitor incoming actions
- Investigate root cause
- Execute corrective action
- Confirm resolution

---

## 5. Action Handling Rules

Each action includes:
- priority (low/medium/high)
- owner (branch supervisor)
- evidence (metrics)
- expiry time

High priority actions must be handled SAME DAY.

---

## 6. Feedback Recording (MANDATORY)

All actions must be acknowledged:

```bash
python3 scripts/record_operator_feedback.py \
  --action-path <action_json> \
  --status acknowledged \
  --by "<name>" \
  --note "<resolution>"
