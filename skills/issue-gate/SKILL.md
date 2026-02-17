---
name: issue-gate
description: Enforce GitHub issue preflight before starting implementation work. Use when beginning a new task to match it against open issues, claim an unstarted match, create a new issue when none matches, and abort only when matching is ambiguous or in-progress work belongs to another actor.
---

# Issue Gate

## Overview
Run issue preflight before any new implementation task. Keep GitHub Issues as the source of truth and return one decision: `CREATE`, `CLAIM`, `ABORT_IN_PROGRESS`, or `ABORT_UNCERTAIN`.

## Workflow
1. Prepare inputs:
- repository (`owner/repo`)
- actor login
- task title
- task body file path
2. Fetch open issues using `gh`:

```bash
gh api --paginate "/repos/owner/repo/issues?state=open&per_page=100" | jq -s '
  add
  | [ .[]
      | select(.pull_request | not)
      | {
          number: .number,
          title: .title,
          body: (.body // ""),
          url: .html_url,
          labels: [ .labels[]?.name ],
          assignees: [ .assignees[]?.login ]
        }
    ]'
```

3. Run qualitative LLM matching using task + open issues and produce `ISSUE_GATE_MATCH_JSON`.
4. Derive and emit one decision with this contract:

```json
{
  "decision": "CREATE|CLAIM|ABORT_IN_PROGRESS|ABORT_UNCERTAIN",
  "matched_issue_number": 123,
  "reason": "string",
  "human_action": "string"
}
```

5. Apply gate behavior:
- `CREATE`: create issue and continue task against the created issue.
- `CLAIM`: assign actor and set labels, then continue task against the claimed issue.
- `ABORT_IN_PROGRESS`: stop task and hand off only when the matched issue is in progress and not assigned to the current actor.
- `ABORT_UNCERTAIN`: stop task and request human triage.
6. When creating the PR for the task, reference the issue in the PR description using a closing keyword (for example: `Closes #$ISSUE`).

## LLM Matching Input (required)
Provide qualitative matching from the runtime LLM with `ISSUE_GATE_MATCH_JSON`:

```json
{
  "classification": "clear|ambiguous|none",
  "matched_issue_number": 123,
  "reason": "short explanation"
}
```

Rules:
- `ISSUE_GATE_MATCH_JSON` is required.
- Missing or malformed `ISSUE_GATE_MATCH_JSON` returns `ABORT_UNCERTAIN`.
- `classification=clear` requires a positive integer `matched_issue_number`.

## Decision Mapping
- `classification=ambiguous` -> `ABORT_UNCERTAIN`.
- `classification=none` -> `CREATE`.
- `classification=clear`:
  - If matched issue is in progress and assigned to someone other than `ACTOR` -> `ABORT_IN_PROGRESS`.
  - If matched issue is in progress and assigned to `ACTOR` -> `CLAIM`.
  - If matched issue is not started -> `CLAIM`.

In-progress signal:
- Issue has label `status:in-progress`, or
- Issue has one or more assignees.

Ownership rule:
- If any assignee login equals `ACTOR`, treat issue as owned by current actor and continue (`CLAIM`).
- If issue is in progress and no assignee matches `ACTOR`, abort (`ABORT_IN_PROGRESS`).

## GH Commands
- Create issue:
  - `gh api -X POST "/repos/$REPO/issues" -f "title=$TASK_TITLE" -F "body=@$TASK_BODY_FILE"`
- Claim issue:
  - `gh api -X POST "/repos/$REPO/issues/$ISSUE/assignees" -f "assignees[]=$ACTOR"`
  - `gh api -X POST "/repos/$REPO/issues/$ISSUE/labels" -f "labels[]=status:in-progress" || true`
  - `gh api -X DELETE "/repos/$REPO/issues/$ISSUE/labels/status:todo" >/dev/null 2>&1 || true`

## References
- Matching rubric: `references/match-rubric.md`
- Manual Claude usage: `references/claude-usage.md`
