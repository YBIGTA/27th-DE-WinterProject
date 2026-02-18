# Markdown Migration Plan (Historical)

## Status
Applied on 2026-02-16.
Moved to `docs/history/` on 2026-02-18.

## Scope
1. Centralized runbooks/history docs under `docs/`.
2. Merged `history` + `changelog` into `docs/history/`.
3. Renamed `plans` folder intent to `docs/current/`.
4. Kept `EXPLANATION.md` / `Explanation.md` naming for component design docs.
5. Folded component runtime ownership matrix into `config/EXPLANATION.md` (removed `docs/reference/`).

## Final Structure
```text
docs/
  current/
  history/
  runbooks/
```

## Key Mappings
1. `config/VALIDATION.md` -> `docs/runbooks/validation.md`
2. `config/COMPONENT-CONFIGS.md` -> merged into `config/EXPLANATION.md`
3. `config/CONFIG-UPGRADE-PLAN.md` -> `docs/history/config-upgrade-plan-2026-02-04.md`
4. `docs/runtime-runbook.md` + `config/README.md` -> `docs/runbooks/runtime.md`
5. `docs/optimization-changelog.md` -> `docs/history/pipeline-optimization-2026-02-14.md`
6. `demolish-ops.md` -> `docs/history/demolish-ops.md`
7. `merge-issue.md` -> `docs/history/merge-issues-2026-02-04.md`
