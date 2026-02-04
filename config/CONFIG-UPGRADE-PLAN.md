---
status: COMPLETED-HISTORICAL
created: 2026-02-04
last_updated: 2026-02-04
purpose: phase-2-config-consolidation-history
superseded_by:
  - demolish-ops.md
  - config/README.md
---

# Config Upgrade Plan (Historical)

이 문서는 Phase 2(컴포넌트 YAML 중심 모델) 완료 기록이다.
현재 운영 기준은 Demolish Ops 이후 문서(`config/README.md`)를 따른다.

## 참고
- Phase 2에서는 runtime YAML 기반 구조를 도입했지만,
  이후 Demolish Ops 단계에서 배포 서비스 런타임 설정은 compose 하드코딩 방식으로 전환되었다.
- 네트워크 설정을 `config/.env`로 제한하는 원칙은 유지된다.
