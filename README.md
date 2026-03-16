# Oracle SQL 튜닝 자동화 파이프라인

Oracle Database에서 느린 SQL을 자동으로 감지하고, 10046/10053 트레이스를 수집하여 성능 분석 보고서를 생성하는 파이프라인입니다.

## 주요 기능

### Phase 1: 느린 SQL 감지
- V$SQL 뷰에서 성능 임계값을 초과하는 SQL 자동 감지
- 실행 시간, CPU 사용량, Buffer Gets 등 다양한 기준 적용

### Phase 2: 트레이스 수집
- **10046 트레이스**: SQL 실행 과정의 바인드 변수와 대기 이벤트 수집
- **10053 트레이스**: 옵티마이저 실행계획 선택 과정 수집 (NEW!)

### Phase 3: 분석
- tkprof를 통한 10046 트레이스 분석
- 정규식 기반 10053 트레이스 파싱 및 분석
- 옵티마이저 결정 과정의 비용 분석

### Phase 4: 보고서 생성
- HTML 종합 보고서 (10053 분석 결과 포함)
- Excel 형태의 상세 분석 보고서
- 자동 권고사항 생성

## 새로 추가된 10053 기능

### 10053 vs 10046 차이점
- **10046**: SQL 실행 시 실제 일어나는 일들을 추적 (바인드 변수, 대기 이벤트)
- **10053**: 옵티마이저가 실행계획을 어떻게 선택했는지 분석 (비용 계산 과정)

### 10053 분석 내용
- 옵티마이저 파라미터 정보
- 테이블/인덱스 통계 정보
- 각 테이블별 접근 방법과 비용
- 조인 순서 후보들과 비용 비교
- 최종 선택된 실행계획과 이유
- 개선 포인트 자동 감지

### 자동 이슈 감지
- 통계 정보 부정확성
- 카디널리티 추정 오차
- 비효율적인 조인 순서
- Full Table Scan 선택 이유

## 프로젝트 구조

```
D:\oracle-sql-tuning\
├── main.py                     # 통합 실행기 (10053 명령 추가)
├── config/
│   └── settings.yaml          # 전체 설정 (10053 설정 포함)
├── python/
│   ├── slow_sql_detector.py   # Phase 1: 느린 SQL 감지
│   ├── trace_collector.py     # Phase 2: 10046 트레이스 수집
│   ├── optimizer_trace.py     # Phase 2-B: 10053 트레이스 수집/분석 (NEW!)
│   ├── tkprof_analyzer.py     # Phase 3: tkprof 분석
│   ├── report_generator.py    # Phase 4: 종합 보고서 (10053 포함)
│   ├── export_to_excel.py     # 엑셀 내보내기 (10053 시트 추가)
│   └── utils.py               # 공통 유틸리티
└── output/                    # 출력 디렉터리
    ├── traces/               # 트레이스 파일
    ├── reports/              # HTML 보고서
    ├── excel/                # Excel 보고서
    └── logs/                 # 로그 파일
```

## 설치 및 설정

### 1. Python 의존성
```bash
pip install oracledb pandas openpyxl paramiko jinja2 pyyaml
```

### 2. Oracle 클라이언트
- Oracle Instant Client 설치 필요 (tkprof 명령 포함)
- PATH에 Oracle 클라이언트 경로 추가

### 3. SSH 키 설정
- Oracle 서버에 SSH 키 기반 접속 설정
- 트레이스 파일 수집용

### 4. 설정 파일 수정
`config/settings.yaml`에서 다음 항목들 설정:
- 데이터베이스 접속 정보
- SSH 접속 정보
- 트레이스 디렉터리 경로
- 10053 분석 규칙

## 사용법

### 전체 파이프라인 실행 (10053 포함)
```bash
python main.py run --with-10053
```

### 특정 SQL에 대해서만 10053 분석
```bash
python main.py optimizer-trace --sql-id abc123def456
```

### 기존 10053 트레이스 파일 분석
```bash
python main.py optimizer-analyze --file output/traces/10053_abc123.trc
```

### Phase별 개별 실행
```bash
# Phase 1만 실행 (느린 SQL 감지)
python main.py phase1

# Phase 2만 실행 (특정 SQL 트레이스 수집)
python main.py phase2 --sql-id abc123def456

# Phase 3만 실행 (tkprof 분석)
python main.py phase3 --trace-file output/traces/trace_abc123.trc
```

## 설정 옵션

### 10053 옵티마이저 트레이스 설정
```yaml
optimizer_trace:
  enabled: true
  collection_method: "explain"    # explain | execute
  retention_days: 30
  
  limits:
    max_concurrent: 2
    timeout_minutes: 15
    max_file_size_mb: 100
  
  analysis_rules:
    cardinality_error_threshold: 10
    stale_stats_days: 30
    cost_diff_threshold: 20
```

### 감지 임계값 설정
```yaml
slow_sql_detection:
  thresholds:
    elapsed_time_ms: 5000      # 5초 이상
    cpu_time_ms: 3000          # CPU 3초 이상
    executions: 10             # 실행 횟수 10회 이상
    buffer_gets_per_exec: 10000
```

## 출력 결과

### HTML 보고서
- 느린 SQL 요약
- 트레이스 분석 결과
- **10053 옵티마이저 분석** (NEW!)
  - 비용 분석 통계
  - 개별 SQL 분석 결과
  - 공통 이슈 패턴
- 종합 권고사항

### Excel 보고서
- 요약 시트
- 느린 SQL 요약 시트
- 트레이스 분석 시트
- **옵티마이저 결정 시트** (NEW!)
  - 10053 분석 요약
  - 개별 SQL 분석 결과
  - 이슈 유형별 통계
- 개선 권고사항 시트

### 10053 개별 HTML 리포트
각 SQL별로 상세한 옵티마이저 분석 리포트:
- 옵티마이저 파라미터
- 테이블 통계 정보
- 테이블 접근 경로별 비용
- 조인 순서 후보 비교
- 발견된 이슈와 개선 방안

## 주의사항

1. **10053 트레이스 수집 시 주의**:
   - 10053은 상당한 오버헤드를 발생시킬 수 있음
   - 운영 환경에서는 `collection_method: "explain"` 사용 권장
   - 동시 수집 개수 제한 설정 (기본 2개)

2. **권한 요구사항**:
   - ALTER SESSION 권한 필요
   - V$SQL 뷰 조회 권한
   - 트레이스 디렉터리 접근 권한 (SSH)

3. **디스크 사용량**:
   - 10053 트레이스 파일은 크기가 클 수 있음
   - `retention_days` 설정으로 자동 정리

## 문제 해결

### 트레이스 파일을 찾을 수 없는 경우
- `config/settings.yaml`의 `trace_directory` 경로 확인
- SSH 접속 및 권한 확인
- Oracle의 `user_dump_dest` 파라미터 확인

### 10053 분석 결과가 비어있는 경우
- SQL이 실제로 복잡한 조인을 포함하는지 확인
- 단순한 SQL은 10053 정보가 적을 수 있음
- `collection_method: "execute"`로 변경 시도 (주의)

### tkprof 명령을 찾을 수 없는 경우
- Oracle 클라이언트가 설치되어 있는지 확인
- PATH 환경변수에 Oracle 클라이언트 경로 추가

## 라이선스

이 프로젝트는 내부 사용을 위한 도구입니다.

## 기여

버그 리포트나 기능 개선 제안은 이슈로 등록해 주세요.