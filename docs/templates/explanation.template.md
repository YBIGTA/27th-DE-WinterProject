# EXPLANATION.md

너는 **Documentation-Driven Developer**다. 아래 규칙에 따라 코드를 분석하고 EXPLANATION.md를 생성/업데이트하라.

## 작업 순서
1. 기존 EXPLANATION.md가 있으면 먼저 읽고 실제 코드와 비교
2. 불일치 발견 시 → 문서 업데이트 최우선
3. 없으면 → 새로 생성

## 분석 중점 사항
- **Concurrency:** synchronized, Lock, CompletableFuture, volatile 등 동기화 방식 반드시 명시
- **State:** 어디에 상태가 저장되고 어떻게 관리되는지
- **Data Flow:** 입력부터 출력까지 전체 흐름
  - LB/프록시/브로커 같은 **중간 홉을 생략하지 말 것**

## 출력 형식

---
component: [컴포넌트명]
status: CURRENT
last_reviewed: [오늘 날짜]
core_files:
  - [분석한 파일 경로들]
---

# [컴포넌트명]

## Role
[이 컴포넌트가 존재하는 이유 - 한 문장]

## I/O Flow
```
[Upstream] --(Protocol)--> [This Component] --(Protocol)--> [Gateway/LB/Proxy (if any)] --(Protocol)--> [Downstream]
```

## Implementation Logic

### Data Flow
```mermaid
flowchart TD
    [실제 코드 흐름 기반으로 작성]
```

### Concurrency Model
- **Thread Model:** [Single-threaded / Thread-pool / Event-loop / etc.]
- **Shared State:** [상태 저장 위치와 보호 방식]
- **Sync Primitives:** [사용된 동기화 도구들]

### Core Algorithm
[핵심 처리 로직 - 코드의 실제 동작 기반]

## Data Contract
- **Input:** [형식과 필드]
- **Output:** [형식과 필드]
- **Invariants:** [반드시 지켜져야 하는 조건]

## Design Decisions
| Decision | Why | Trade-off |
|----------|-----|-----------|
| [선택] | [이유] | [포기한 것] |

## Failure Modes & Handling
| Failure | Detection | Response |
|---------|-----------|----------|
| [장애 유형] | [감지 방법] | [대응] |
