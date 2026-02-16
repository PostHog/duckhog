# Match Rubric

Use this qualitative rubric when mapping a task to open issues.

## Classifications
- `clear`: one issue is the obvious same work item.
- `ambiguous`: two or more issues are plausible matches.
- `none`: no issue plausibly represents the task.

## How to Decide
1. Compare task title and task body to issue title and body.
2. Prefer semantic intent over exact wording.
3. Consider scope, acceptance criteria, and ownership hints.
4. Ignore closed issues for this workflow; compare against open issues only.

## Tie-Break Guidance
- If two issues overlap and one is broader while another is precise, prefer the precise issue only when it clearly contains the requested work.
- If confidence is not clearly one-way, classify as `ambiguous`.
- Never force a match to avoid creating an issue.

## Required Outcome Mapping
- `clear` and issue not started -> claim issue.
- `clear` and issue in progress, assigned to current actor -> claim issue.
- `clear` and issue in progress, not assigned to current actor -> `ABORT_IN_PROGRESS`.
- `ambiguous` -> `ABORT_UNCERTAIN` and request human triage.
- `none` -> create issue.
