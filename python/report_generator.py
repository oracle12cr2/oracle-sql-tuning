#!/usr/bin/env python3
"""
Phase 4: 일간/주간 정기 튜닝 리포트 (report_generator.py)

AWR 스냅샷에서 Top SQL을 추출하고,
Phase 1~3 결과를 통합하여 종합 튜닝 리포트를 생성한다.

주요 기능:
1. AWR Top SQL 추출 (DBA_HIST_SQLSTAT)
2. Phase 1~3 결과 통합
3. 일간/주간 HTML 대시보드 리포트 생성
4. (선택) 엑셀 산출물 생성
5. (선택) 이메일 발송

사용법:
    python3 report_generator.py --daily         # 일간 리포트
    python3 report_generator.py --weekly        # 주간 리포트
    python3 report_generator.py --date 2025-03-01  # 특정 날짜
"""

import argparse
import glob
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from utils import load_config, setup_logger, get_oracle_connection


# ============================================
# AWR Top SQL 추출
# ============================================
AWR_TOP_SQL_QUERY = """
SELECT /*+ tuning_auto */
    ss.sql_id,
    ss.plan_hash_value,
    DBMS_LOB.SUBSTR(st.sql_text, 4000, 1) AS sql_text,
    ss.parsing_schema_name,
    SUM(ss.executions_delta)                AS executions,
    ROUND(SUM(ss.elapsed_time_delta) / 1e6, 2) AS elapsed_sec,
    CASE WHEN SUM(ss.executions_delta) > 0
         THEN ROUND(SUM(ss.elapsed_time_delta) / SUM(ss.executions_delta) / 1e6, 3)
         ELSE ROUND(SUM(ss.elapsed_time_delta) / 1e6, 3)
    END                                     AS elapsed_per_exec,
    SUM(ss.buffer_gets_delta)               AS buffer_gets,
    CASE WHEN SUM(ss.executions_delta) > 0
         THEN ROUND(SUM(ss.buffer_gets_delta) / SUM(ss.executions_delta))
         ELSE SUM(ss.buffer_gets_delta)
    END                                     AS buffer_gets_per_exec,
    SUM(ss.disk_reads_delta)                AS disk_reads,
    ROUND(SUM(ss.cpu_time_delta) / 1e6, 2) AS cpu_sec,
    SUM(ss.rows_processed_delta)            AS rows_processed,
    SUM(ss.iowait_delta) / 1e6             AS io_wait_sec
FROM dba_hist_sqlstat ss
JOIN dba_hist_snapshot sn
    ON ss.snap_id = sn.snap_id
    AND ss.instance_number = sn.instance_number
    AND ss.dbid = sn.dbid
LEFT JOIN dba_hist_sqltext st
    ON ss.sql_id = st.sql_id
    AND ss.dbid = st.dbid
WHERE sn.begin_interval_time >= :start_time
  AND sn.end_interval_time <= :end_time
  AND ss.parsing_schema_name NOT IN ('SYS', 'SYSTEM', 'DBSNMP', 'OUTLN')
GROUP BY ss.sql_id, ss.plan_hash_value, DBMS_LOB.SUBSTR(st.sql_text, 4000, 1), ss.parsing_schema_name
ORDER BY elapsed_sec DESC
FETCH FIRST :top_n ROWS ONLY
"""

# 11g 호환 버전 (FETCH FIRST 미지원)
AWR_TOP_SQL_QUERY_11G = """
SELECT * FROM (
    SELECT /*+ tuning_auto */
        ss.sql_id,
        ss.plan_hash_value,
        DBMS_LOB.SUBSTR(st.sql_text, 4000, 1) AS sql_text,
        ss.parsing_schema_name,
        SUM(ss.executions_delta)                AS executions,
        ROUND(SUM(ss.elapsed_time_delta) / 1e6, 2) AS elapsed_sec,
        CASE WHEN SUM(ss.executions_delta) > 0
             THEN ROUND(SUM(ss.elapsed_time_delta) / SUM(ss.executions_delta) / 1e6, 3)
             ELSE ROUND(SUM(ss.elapsed_time_delta) / 1e6, 3)
        END                                     AS elapsed_per_exec,
        SUM(ss.buffer_gets_delta)               AS buffer_gets,
        CASE WHEN SUM(ss.executions_delta) > 0
             THEN ROUND(SUM(ss.buffer_gets_delta) / SUM(ss.executions_delta))
             ELSE SUM(ss.buffer_gets_delta)
        END                                     AS buffer_gets_per_exec,
        SUM(ss.disk_reads_delta)                AS disk_reads,
        ROUND(SUM(ss.cpu_time_delta) / 1e6, 2) AS cpu_sec,
        SUM(ss.rows_processed_delta)            AS rows_processed,
        SUM(ss.iowait_delta) / 1e6             AS io_wait_sec
    FROM dba_hist_sqlstat ss
    JOIN dba_hist_snapshot sn
        ON ss.snap_id = sn.snap_id
        AND ss.instance_number = sn.instance_number
        AND ss.dbid = sn.dbid
    LEFT JOIN dba_hist_sqltext st
        ON ss.sql_id = st.sql_id
        AND ss.dbid = st.dbid
    WHERE sn.begin_interval_time >= :start_time
      AND sn.end_interval_time <= :end_time
      AND ss.parsing_schema_name NOT IN ('SYS', 'SYSTEM', 'DBSNMP', 'OUTLN')
    GROUP BY ss.sql_id, ss.plan_hash_value, DBMS_LOB.SUBSTR(st.sql_text, 4000, 1), ss.parsing_schema_name
    ORDER BY elapsed_sec DESC
)
WHERE ROWNUM <= :top_n
"""


def get_db_version(conn):
    """Oracle 버전 확인"""
    cursor = conn.cursor()
    cursor.execute("SELECT version FROM v$instance")
    version = cursor.fetchone()[0]
    cursor.close()
    major = int(version.split(".")[0])
    return major


def fetch_awr_top_sql(config, logger, start_time, end_time):
    """AWR에서 Top SQL 추출"""
    top_n = config["report"]["awr"]["top_n"]

    conn = get_oracle_connection(config)
    cursor = conn.cursor()

    try:
        db_version = get_db_version(conn)
        query = AWR_TOP_SQL_QUERY if db_version >= 12 else AWR_TOP_SQL_QUERY_11G

        logger.info(f"  AWR 조회: {start_time} ~ {end_time} (Top {top_n})")
        cursor.execute(query, {
            "start_time": start_time,
            "end_time": end_time,
            "top_n": top_n,
        })

        columns = [col[0].lower() for col in cursor.description]
        results = []
        for row in cursor:
            item = dict(zip(columns, row))
            # CLOB 처리
            if item.get("sql_text") and hasattr(item["sql_text"], "read"):
                item["sql_text"] = item["sql_text"].read()
            results.append(item)

        logger.info(f"  AWR Top SQL: {len(results)}건")
        return results

    finally:
        cursor.close()
        conn.close()


def fetch_awr_db_summary(config, logger, start_time, end_time):
    """AWR DB 전체 요약 통계"""
    conn = get_oracle_connection(config)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT /*+ tuning_auto */
                ROUND(SUM(e.value - b.value) / 1e6, 2) AS db_time_sec,
                (SELECT ROUND(SUM(elapsed_time_delta) / 1e6, 2)
                 FROM dba_hist_sqlstat ss
                 JOIN dba_hist_snapshot sn ON ss.snap_id = sn.snap_id
                    AND ss.instance_number = sn.instance_number
                    AND ss.dbid = sn.dbid
                 WHERE sn.begin_interval_time >= :start_time
                   AND sn.end_interval_time <= :end_time) AS sql_elapsed_sec
            FROM dba_hist_sysstat b
            JOIN dba_hist_sysstat e ON b.stat_name = e.stat_name
                AND b.instance_number = e.instance_number
                AND b.dbid = e.dbid
            JOIN dba_hist_snapshot sn_b ON b.snap_id = sn_b.snap_id
                AND b.instance_number = sn_b.instance_number
            JOIN dba_hist_snapshot sn_e ON e.snap_id = sn_e.snap_id
                AND e.instance_number = sn_e.instance_number
            WHERE b.stat_name = 'DB time'
              AND sn_b.begin_interval_time >= :start_time
              AND sn_e.end_interval_time <= :end_time
              AND e.snap_id = b.snap_id + 1
        """, {"start_time": start_time, "end_time": end_time})

        row = cursor.fetchone()
        return {
            "db_time_sec": row[0] if row and row[0] else 0,
            "sql_elapsed_sec": row[1] if row and row[1] else 0,
        }
    except Exception as e:
        logger.warning(f"  DB 요약 통계 조회 실패: {e}")
        return {"db_time_sec": 0, "sql_elapsed_sec": 0}
    finally:
        cursor.close()
        conn.close()


# ============================================
# Phase 1~3 결과 수집
# ============================================
def collect_phase_results(config, logger, start_time, end_time):
    """이전 Phase 결과 파일들을 수집하여 통합"""
    trace_dir = Path(config["paths"]["trace_output"])
    report_dir = Path(config["paths"]["report_output"])

    # 감지 결과 수집
    detected_files = sorted(trace_dir.glob("detected_*.json"))
    detected_sqls = []
    for f in detected_files:
        try:
            mtime = datetime.fromtimestamp(f.stat().st_mtime)
            if start_time <= mtime <= end_time:
                with open(f, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                    detected_sqls.extend(data.get("sql_list", []))
        except Exception:
            continue

    # tkprof 리포트 수집
    tkprof_reports = sorted(report_dir.glob("tkprof_*.html"))
    phase3_count = sum(
        1 for f in tkprof_reports
        if start_time <= datetime.fromtimestamp(f.stat().st_mtime) <= end_time
    )

    logger.info(f"  Phase 결과 수집: 감지={len(detected_sqls)}건, tkprof리포트={phase3_count}건")

    return {
        "detected_sqls": detected_sqls,
        "tkprof_report_count": phase3_count,
    }


# ============================================
# HTML 대시보드 리포트
# ============================================
def generate_dashboard_report(
    report_type, awr_top_sql, db_summary, phase_results,
    start_time, end_time, output_path, logger
):
    """종합 대시보드 HTML 리포트 생성"""

    title = "일간" if report_type == "daily" else "주간"
    date_range = f"{start_time.strftime('%Y-%m-%d %H:%M')} ~ {end_time.strftime('%Y-%m-%d %H:%M')}"
    gen_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    detected_count = len(phase_results.get("detected_sqls", []))

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<title>Oracle SQL 튜닝 {title} 리포트</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: 'Malgun Gothic', 'Apple SD Gothic Neo', sans-serif; margin: 0; padding: 20px; background: #f0f2f5; color: #333; }}
  .container {{ max-width: 1400px; margin: 0 auto; }}

  /* 헤더 */
  .header {{ background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); color: white; padding: 24px 32px; border-radius: 12px; margin-bottom: 20px; }}
  .header h1 {{ margin: 0 0 4px 0; font-size: 24px; }}
  .header .subtitle {{ opacity: 0.7; font-size: 14px; }}

  /* 요약 카드 */
  .kpi-row {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin-bottom: 20px; }}
  .kpi {{ background: white; padding: 20px; border-radius: 10px; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }}
  .kpi .value {{ font-size: 32px; font-weight: bold; margin: 8px 0; }}
  .kpi .label {{ font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; }}
  .kpi .delta {{ font-size: 12px; margin-top: 4px; }}
  .kpi .delta.up {{ color: #E24B4A; }}
  .kpi .delta.down {{ color: #639922; }}

  /* 섹션 */
  .section {{ background: white; border-radius: 10px; margin-bottom: 16px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); overflow: hidden; }}
  .section-header {{ padding: 16px 20px; border-bottom: 1px solid #eee; display: flex; justify-content: space-between; align-items: center; }}
  .section-header h2 {{ margin: 0; font-size: 17px; }}
  .section-body {{ padding: 16px 20px; }}

  /* 테이블 */
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ background: #f8f9fa; padding: 10px 12px; text-align: left; border-bottom: 2px solid #dee2e6; font-weight: 600; position: sticky; top: 0; }}
  td {{ padding: 10px 12px; border-bottom: 1px solid #f0f0f0; }}
  tr:hover td {{ background: #f8f9fa; }}
  .text-right {{ text-align: right; }}
  .text-center {{ text-align: center; }}
  .mono {{ font-family: 'Consolas', 'Monaco', monospace; font-size: 12px; }}
  .sql-preview {{ max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; cursor: pointer; }}
  .sql-preview:hover {{ white-space: normal; word-break: break-all; }}

  /* 뱃지 */
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: bold; color: white; }}
  .badge-high {{ background: #E24B4A; }}
  .badge-medium {{ background: #EF9F27; }}
  .badge-low {{ background: #639922; }}
  .badge-info {{ background: #378ADD; }}

  /* 랭킹 */
  .rank {{ display: inline-flex; align-items: center; justify-content: center; width: 24px; height: 24px; border-radius: 50%; font-size: 12px; font-weight: bold; }}
  .rank-1 {{ background: #FFD700; color: #333; }}
  .rank-2 {{ background: #C0C0C0; color: #333; }}
  .rank-3 {{ background: #CD7F32; color: white; }}
  .rank-n {{ background: #eee; color: #666; }}

  /* 푸터 */
  .footer {{ text-align: center; padding: 16px; font-size: 12px; color: #999; }}
</style>
</head>
<body>
<div class="container">

  <!-- 헤더 -->
  <div class="header">
    <h1>Oracle SQL 튜닝 {title} 리포트</h1>
    <div class="subtitle">{date_range} | 생성: {gen_time}</div>
  </div>

  <!-- KPI 요약 -->
  <div class="kpi-row">
    <div class="kpi">
      <div class="label">AWR Top SQL</div>
      <div class="value">{len(awr_top_sql)}</div>
    </div>
    <div class="kpi">
      <div class="label">자동 감지 SQL</div>
      <div class="value">{detected_count}</div>
    </div>
    <div class="kpi">
      <div class="label">tkprof 분석</div>
      <div class="value">{phase_results.get('tkprof_report_count', 0)}</div>
    </div>
    <div class="kpi">
      <div class="label">DB Time</div>
      <div class="value">{db_summary.get('db_time_sec', 0):,.0f}<span style="font-size:14px">s</span></div>
    </div>
    <div class="kpi">
      <div class="label">SQL Elapsed</div>
      <div class="value">{db_summary.get('sql_elapsed_sec', 0):,.0f}<span style="font-size:14px">s</span></div>
    </div>
  </div>

  <!-- AWR Top SQL -->
  <div class="section">
    <div class="section-header">
      <h2>AWR Top SQL (by Elapsed Time)</h2>
      <span class="badge badge-info">Top {len(awr_top_sql)}</span>
    </div>
    <div class="section-body" style="overflow-x: auto;">
      <table>
        <thead>
          <tr>
            <th class="text-center">#</th>
            <th>SQL ID</th>
            <th>Schema</th>
            <th class="text-right">Elapsed(s)</th>
            <th class="text-right">Per Exec(s)</th>
            <th class="text-right">Execs</th>
            <th class="text-right">Buffer Gets</th>
            <th class="text-right">Gets/Exec</th>
            <th class="text-right">Disk Reads</th>
            <th class="text-right">CPU(s)</th>
            <th>SQL Text</th>
          </tr>
        </thead>
        <tbody>
"""

    for idx, sql in enumerate(awr_top_sql, 1):
        rank_class = f"rank-{idx}" if idx <= 3 else "rank-n"
        sql_text = str(sql.get("sql_text", "") or "")[:120]
        elapsed = sql.get("elapsed_sec", 0) or 0
        per_exec = sql.get("elapsed_per_exec", 0) or 0

        # 심각도 판정
        severity = ""
        if per_exec >= 10:
            severity = '<span class="badge badge-high">SLOW</span>'
        elif per_exec >= 5:
            severity = '<span class="badge badge-medium">WARN</span>'

        html += f"""          <tr>
            <td class="text-center"><span class="rank {rank_class}">{idx}</span></td>
            <td class="mono">{sql.get('sql_id', '')}</td>
            <td>{sql.get('parsing_schema_name', '')}</td>
            <td class="text-right">{elapsed:,.2f}</td>
            <td class="text-right">{per_exec:,.3f} {severity}</td>
            <td class="text-right">{sql.get('executions', 0):,}</td>
            <td class="text-right">{sql.get('buffer_gets', 0):,}</td>
            <td class="text-right">{sql.get('buffer_gets_per_exec', 0):,}</td>
            <td class="text-right">{sql.get('disk_reads', 0):,}</td>
            <td class="text-right">{sql.get('cpu_sec', 0):,.2f}</td>
            <td class="sql-preview mono">{sql_text}</td>
          </tr>
"""

    html += """        </tbody>
      </table>
    </div>
  </div>
"""

    # 자동 감지 SQL 섹션
    detected_sqls = phase_results.get("detected_sqls", [])
    if detected_sqls:
        html += """
  <div class="section">
    <div class="section-header">
      <h2>자동 감지된 느린 SQL (Phase 1)</h2>
    </div>
    <div class="section-body" style="overflow-x: auto;">
      <table>
        <thead>
          <tr>
            <th class="text-center">#</th>
            <th>SQL ID</th>
            <th>Schema</th>
            <th class="text-right">Elapsed/Exec(s)</th>
            <th class="text-right">Buffer Gets/Exec</th>
            <th class="text-right">Disk Reads/Exec</th>
            <th>SQL Text</th>
          </tr>
        </thead>
        <tbody>
"""
        for idx, sql in enumerate(detected_sqls[:50], 1):
            sql_text = str(sql.get("sql_text", "") or "")[:100]
            html += f"""          <tr>
            <td class="text-center">{idx}</td>
            <td class="mono">{sql.get('sql_id', '')}</td>
            <td>{sql.get('parsing_schema_name', '')}</td>
            <td class="text-right">{sql.get('elapsed_sec_per_exec', 0):,.2f}</td>
            <td class="text-right">{sql.get('buffer_gets_per_exec', 0):,}</td>
            <td class="text-right">{sql.get('disk_reads_per_exec', 0):,}</td>
            <td class="sql-preview mono">{sql_text}</td>
          </tr>
"""
        html += """        </tbody>
      </table>
    </div>
  </div>
"""

    # 푸터
    html += f"""
  <div class="footer">
    Oracle SQL Tuning Automation | Generated at {gen_time}
  </div>
</div>
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info(f"  대시보드 리포트 생성: {output_path}")
    return output_path


# ============================================
# 메인
# ============================================
def main():
    parser = argparse.ArgumentParser(description="Phase 4: 정기 튜닝 리포트")
    parser.add_argument("--daily", action="store_true", help="일간 리포트")
    parser.add_argument("--weekly", action="store_true", help="주간 리포트")
    parser.add_argument("--date", type=str, help="특정 날짜 (YYYY-MM-DD)")
    parser.add_argument("--config", type=str, help="설정 파일 경로")
    args = parser.parse_args()

    config = load_config(args.config)
    logger = setup_logger("phase4_report", config)

    try:
        now = datetime.now()

        if args.date:
            target_date = datetime.strptime(args.date, "%Y-%m-%d")
        else:
            target_date = now

        if args.weekly:
            report_type = "weekly"
            start_time = target_date - timedelta(days=7)
            end_time = target_date
        else:
            report_type = "daily"
            start_time = target_date.replace(hour=0, minute=0, second=0)
            end_time = target_date.replace(hour=23, minute=59, second=59)

        logger.info("=" * 60)
        logger.info(f"Phase 4: {report_type.upper()} 리포트 생성")
        logger.info(f"  기간: {start_time} ~ {end_time}")

        # 1. AWR Top SQL 추출
        awr_top_sql = fetch_awr_top_sql(config, logger, start_time, end_time)

        # 2. DB 요약
        db_summary = fetch_awr_db_summary(config, logger, start_time, end_time)

        # 3. Phase 1~3 결과 수집
        phase_results = collect_phase_results(config, logger, start_time, end_time)

        # 4. 대시보드 리포트 생성
        report_dir = Path(config["paths"]["report_output"])
        report_dir.mkdir(parents=True, exist_ok=True)

        date_str = target_date.strftime("%Y%m%d")
        report_file = report_dir / f"tuning_{report_type}_{date_str}.html"

        generate_dashboard_report(
            report_type, awr_top_sql, db_summary, phase_results,
            start_time, end_time, str(report_file), logger,
        )

        logger.info(f"Phase 4 완료: {report_file}")

    except Exception as e:
        logger.error(f"Phase 4 오류: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
