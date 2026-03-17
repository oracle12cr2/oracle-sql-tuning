# Oracle SQL Tuning Report Automation

Oracle SQL 튜닝 자동화 파이프라인 — 느린 SQL 감지부터 실행계획 분석, 10053 옵티마이저 트레이스, 통합 Excel 보고서까지 원스톱으로 처리합니다.

## 📋 분석 흐름

```
① SQL_ID 선택      V$SQL 자동 감지 또는 수동 지정
       ↓
② XPLAN            DBMS_XPLAN.DISPLAY_CURSOR (실제 실행 통계)
       ↓
③ 10046 Trace      실행 추적 (Wait Event, Bind Variable)
       ↓
④ 10053 Trace      옵티마이저 의사결정 분석 (Access Path, Cost)
       ↓
⑤ 튜닝 리포트      17시트 Excel + HTML 보고서
```

## 🚀 Quick Start

### 특정 SQL 분석 (가장 자주 사용)
```bash
python main.py target --sql-id <SQL_ID> --db-password <password>
```

### 전체 파이프라인 (자동 감지 → 분석 → 리포트)
```bash
python main.py run --db-password <password>
```

### 리포트만 재생성
```bash
python main.py run --skip-detect --db-password <password>
```

## 📁 프로젝트 구조

```
oracle-sql-tuning/
├── main.py                      # 통합 실행기 (12개 커맨드)
├── config/
│   └── settings.yaml            # DB접속, 임계값, SSH 설정
├── python/
│   ├── utils.py                 # DB접속, 로깅, 설정로드
│   ├── slow_sql_detector.py     # Phase 1: 느린 SQL 감지
│   ├── trace_collector.py       # Phase 2: 10046 트레이스 수집
│   ├── tkprof_analyzer.py       # Phase 3: tkprof 분석
│   ├── report_generator.py      # Phase 4: HTML 리포트
│   ├── optimizer_trace.py       # 10053 수집/파싱/HTML 리포트
│   └── export_to_excel.py       # 17시트 통합 Excel 리포트
├── scripts/                     # 쉘 스크립트 (Linux용)
├── refresh_report.bat           # Windows 더블클릭 실행
├── run_full_pipeline.bat        # Windows 전체 파이프라인
└── output/
    ├── reports/                 # 분석 결과 (날짜_SQL_ID 폴더)
    ├── traces/                  # 트레이스 파일
    └── tkprof/                  # tkprof 결과
```

## 📊 Excel 보고서 시트 구성 (17시트)

| 시트 | 내용 |
|------|------|
| 📄 튜닝 보고서 | 표지 + 분석 결과 요약 |
| 🔌 DB 접속 정보 | V$INSTANCE, V$DATABASE, SGA/PGA, 파라미터 (실시간 조회) |
| 📋 XPLAN 실행계획 | DBMS_XPLAN.DISPLAY_CURSOR (A-Rows, Buffers, Starts) |
| 📋 요약 | AWR SQL별 핵심 지표 |
| 📊 실행계획 | SQL별 실행계획 전문 |
| 📈 AWR 성능통계 | 스냅샷별 상세 성능 지표 |
| 💡 튜닝 가이드 | 자동 분석 기반 튜닝 권장사항 |
| 🔍 감지된 느린 SQL | Phase 1 임계값 초과 SQL |
| 📄 tkprof 원문 | 10046 트레이스 분석 전문 |
| ⏱ Parse-Exec-Fetch | Parse/Execute/Fetch 단계별 통계 |
| ⏳ 대기 이벤트 | Wait Event 분석 |
| 🔖 바인드 변수 | 사용된 바인드 변수 값 |
| 🔬 10053 요약 | 옵티마이저 트레이스 요약 |
| 🛤 접근 경로 | 테이블별 Access Path 비용 비교 |
| 📊 10053 통계 | 시스템/테이블/인덱스/컬럼 통계 |
| ⚠ 10053 이슈 | 옵티마이저 관련 이슈 + 권장사항 |
| ⚙ 옵티마이저 파라미터 | Altered/Default 파라미터 |

## 🔧 명령어 목록

| 명령어 | 설명 |
|--------|------|
| `target --sql-id <ID>` | 특정 SQL_ID 타겟 분석 |
| `run` | 전체 파이프라인 (Phase 1→2→3→4→10053→Excel) |
| `detect` | Phase 1: 느린 SQL 감지 |
| `trace --sql-id <ID>` | Phase 2: 10046 트레이스 수집 |
| `analyze` | Phase 3: tkprof 분석 |
| `report --daily` | Phase 4: HTML 리포트 |
| `optimizer-trace --sql-id <ID>` | 10053 트레이스 수집 |
| `optimizer-analyze` | 10053 분석 |
| `export` | Excel 리포트 생성 |
| `status` | 현재 상태 확인 |
| `cleanup` | 오래된 파일 정리 |

### 옵션

```bash
--db-password <pwd>   # DB 비밀번호
--skip-detect         # Phase 1 스킵
--skip-10046          # 10046 트레이스 스킵
--skip-10053          # 10053 트레이스 스킵
--skip-excel          # Excel 생성 스킵
```

## 📂 출력 폴더 구조

`target` 커맨드 실행 시 날짜+SQL_ID별 폴더에 모든 결과물 저장:

```
output/reports/
└── 2026.03.17_b3p2xsw0as5tr/
    ├── xplan_b3p2xsw0as5tr.txt           # 실행계획
    ├── 10053_b3p2xsw0as5tr.html          # 옵티마이저 분석
    ├── tuning_report_b3p2xsw0as5tr.xlsx  # 17시트 Excel
    └── *.trc                              # 트레이스 원본
```

## ⚙ 설정 (config/settings.yaml)

```yaml
database:
  user: "app_user"
  password_env: "ORACLE_TUNING_PWD"  # 환경변수명
  host: "192.168.50.31"
  port: 1521
  service_name: "PROD"

detection:
  elapsed_threshold_sec: 3      # 경과시간 임계값
  buffer_gets_threshold: 100000 # Buffer Gets 임계값
```

## 📦 Requirements

```bash
pip install oracledb openpyxl pyyaml paramiko
```

- Python 3.9+
- Oracle Database 11g / 12c / 19c / 21c+
- DB 접속 계정에 `V$SQL`, `V$INSTANCE`, `DBA_HIST_*`, `DBMS_XPLAN` 권한 필요

## License

MIT
