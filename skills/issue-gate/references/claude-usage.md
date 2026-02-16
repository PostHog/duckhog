# Manual LLM Matching Usage

Use this when preparing `ISSUE_GATE_MATCH_JSON` for issue-gate preflight.

## Inputs to Provide the LLM
- Task title
- Task body
- Current actor login
- Open issues (number, title, body, labels, assignees)
- Match rubric from `match-rubric.md`

## Required Output JSON
Return exactly one JSON object:

```json
{
  "classification": "clear|ambiguous|none",
  "matched_issue_number": 123,
  "reason": "short explanation"
}
```

Notes:
- `matched_issue_number` is required only when `classification` is `clear`.
- For `ambiguous` or `none`, omit `matched_issue_number` or set it to `null`.
- Keep `reason` concise and specific.

## Export Example

```bash
export ISSUE_GATE_MATCH_JSON='{"classification":"clear","matched_issue_number":29,"reason":"Task scope matches RM06 CREATE VIEW remote support."}'
```
