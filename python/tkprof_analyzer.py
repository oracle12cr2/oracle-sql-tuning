#!/usr/bin/env python3
"""
Phase 3: tkprof 변환 및 분석 리포트 자동 생성 (tkprof_analyzer.py)

트레이스 파일에 대해:
1. tkprof 실행하여 변환
2. tkprof 출력 파싱 (실행계획, 통계 추출)
3. 개선 포인트 자동 태깅 (Full Table Scan, 고비용 NL 조인 등)
4. HTML/Markdown 리포트 생성

사용법:
    python3 tkprof_analyzer.py /path/to/trace.trc
    python3 tkprof_analyzer.py --dir /path/to/traces/  # 디렉토리 전체
"""

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

from utils import load_config, setup_logger


# ============================================
# tkprof 출력 파서
# ============================================
@dataclass
class SQLStatement:
    """파싱된 SQL 문장 정보"""
    sql_text: str = ""
    sql_id: str = ""
    # Parse/Execute/Fetch 통계
    parse_count: int = 0
    parse_cpu: float = 0
    parse_elapsed: float = 0
    parse_disk: int = 0
    parse_query: int = 0
    parse_current: int = 0
    parse_rows: int = 0
    execute_count: int = 0
    execute_cpu: float = 0
    execute_elapsed: float = 0
    execute_disk: int = 0
    execute_query: int = 0
    execute_current: int = 0
    execute_rows: int = 0
    fetch_count: int = 0
    fetch_cpu: float = 0
    fetch_elapsed: float = 0
    fetch_disk: int = 0
    fetch_query: int = 0
    fetch_current: int = 0
    fetch_rows: int = 0
    # 합계
    total_cpu: float = 0
    total_elapsed: float = 0
    total_disk: int = 0
    total_query: int = 0
    total_current: int = 0
    total_rows: int = 0
    # 실행계획
    execution_plan: list = field(default_factory=list)
    # 대기 이벤트
    wait_events: list = field(default_factory=list)
    # 바인드 변수
    bind_variables: list = field(default_factory=list)
    # 개선 포인트
    issues: list = field(default_factory=list)
    # tkprof 블록 원문
    raw_block: str = ""


def run_tkprof(trc_file, output_file, config, logger):
    """
    tkprof 실행 - 로컬 바이너리 없으면 SSH로 리눅스에서 실행 후 결과 파일 수집

    Returns:
        str: tkprof 출력 파일 경로 (성공 시), None (실패 시)
    """
    tkprof_bin = config["tkprof"]["binary_path"]
    sort_opt = config["tkprof"]["sort_option"]
    explain_user = config["tkprof"].get("explain_user", "")

    # 로컬 바이너리 존재 여부 확인
    local_bin_exists = tkprof_bin and os.path.exists(tkprof_bin)

    if not local_bin_exists:
        # SSH로 리눅스에서 tkprof 실행
        return _run_tkprof_remote(trc_file, output_file, config, logger, sort_opt, explain_user)

    cmd = [tkprof_bin, trc_file, output_file, f"sort={sort_opt}"]
    if explain_user:
        cmd.append(f"explain={explain_user}")
    cmd.append("sys=no")

    logger.info(f"  tkprof 실행: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, encoding='utf-8', errors='replace')

        if result.returncode != 0:
            logger.error(f"  tkprof 실패: {result.stderr}")
            return None

        if not os.path.exists(output_file):
            logger.error(f"  tkprof 출력 파일 생성 안 됨: {output_file}")
            return None

        logger.info(f"  tkprof 변환 완료: {output_file}")
        return output_file

    except subprocess.TimeoutExpired:
        logger.error("  tkprof 타임아웃 (120초)")
        return None
    except FileNotFoundError:
        logger.error(f"  tkprof 바이너리 없음: {tkprof_bin}")
        return None


def _run_tkprof_remote(trc_file, output_file, config, logger, sort_opt, explain_user):
    """
    SSH로 리눅스 서버에서 tkprof 실행 후 결과 파일을 로컬로 수집
    """
    try:
        from trace_collector import SSHClient
    except ImportError:
        logger.error("  SSH 모듈 로드 실패 (trace_collector.py 필요)")
        return None

    ssh = SSHClient(config, logger)
    if not ssh.enabled:
        logger.error("  SSH 비활성화 상태 - tkprof 원격 실행 불가")
        return None

    ok, msg = ssh.test_connection()
    if not ok:
        logger.error(f"  SSH 접속 실패: {msg}")
        return None

    # 로컬 trc 파일을 리눅스로 업로드
    trc_path = Path(trc_file)
    remote_tmp = f"/tmp/{trc_path.name}"
    remote_prf = f"/tmp/{trc_path.stem}.prf"

    logger.info(f"  trc 파일 업로드: {trc_path.name} → {remote_tmp}")

    # SFTP로 업로드
    try:
        import paramiko
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        connect_kwargs = {"hostname": ssh.host, "port": ssh.port, "username": ssh.user}
        if ssh.auth_method == "key" and os.path.exists(ssh.key_path):
            connect_kwargs["key_filename"] = ssh.key_path
        elif ssh.password:
            connect_kwargs["password"] = ssh.password
        ssh_client.connect(**connect_kwargs)

        sftp = ssh_client.open_sftp()
        sftp.put(str(trc_file), remote_tmp)

        # tkprof 실행 (전체 경로 우선, 없으면 ORACLE_HOME 탐색)
        sort_clause = f"sort={sort_opt}"
        explain_clause = f"explain={explain_user}" if explain_user else ""

        # SSH non-login shell은 PATH가 제한적이므로 전체 경로로 실행
        find_cmd = "find /oracle/app/oracle/product -name tkprof -type f 2>/dev/null | head -1"
        stdin2, stdout2, _ = ssh_client.exec_command(find_cmd, timeout=15)
        tkprof_remote = stdout2.read().decode().strip() or "tkprof"

        remote_cmd = f"{tkprof_remote} {remote_tmp} {remote_prf} {sort_clause} {explain_clause} sys=no"
        logger.info(f"  원격 tkprof 실행: {remote_cmd}")

        stdin, stdout, stderr = ssh_client.exec_command(remote_cmd, timeout=120)
        exit_status = stdout.channel.recv_exit_status()
        err_output = stderr.read().decode().strip()

        if exit_status != 0:
            logger.error(f"  원격 tkprof 실패 (exit={exit_status}): {err_output}")
            ssh_client.close()
            return None

        # 결과 파일 다운로드
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        sftp.get(remote_prf, output_file)

        # 임시 파일 정리
        ssh_client.exec_command(f"rm -f {remote_tmp} {remote_prf}")
        sftp.close()
        ssh_client.close()

        file_size = os.path.getsize(output_file) / 1024
        logger.info(f"  원격 tkprof 완료: {Path(output_file).name} ({file_size:.1f}KB)")
        return output_file

    except Exception as e:
        logger.error(f"  원격 tkprof 오류: {e}")
        return None


def parse_tkprof_output(tkprof_file, logger):
    """
    tkprof 출력 파일 파싱

    Returns:
        tuple: (list[SQLStatement], str) - 파싱된 SQL 목록, tkprof 전체 텍스트
    """
    with open(tkprof_file, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()

    statements = []
    blocks = re.split(r"\*{10,}", content)

    for block in blocks:
        block = block.strip()
        if not block or len(block) < 50:
            continue

        stmt = parse_sql_block(block, logger)
        if stmt and stmt.sql_text and stmt.total_elapsed > 0:
            statements.append(stmt)

    logger.info(f"  파싱 완료: {len(statements)}개 SQL 문장")
    return statements, content


def parse_sql_block(block, logger):
    """단일 SQL 블록 파싱"""
    stmt = SQLStatement()
    stmt.raw_block = block  # 원문 보존

    lines = block.split("\n")

    # 1. SQL 텍스트 추출 (call count 테이블 전까지)
    sql_lines = []
    stat_start = -1
    for i, line in enumerate(lines):
        if re.match(r"^call\s+count", line, re.IGNORECASE):
            stat_start = i
            break
        # SQL ID 추출
        sql_id_match = re.search(r"sql_id='(\w+)'", line)
        if sql_id_match:
            stmt.sql_id = sql_id_match.group(1)
            continue
        sql_lines.append(line)

    stmt.sql_text = "\n".join(sql_lines).strip()

    if stat_start < 0:
        return None

    # 2. 통계 테이블 파싱 (Parse/Execute/Fetch/Total)
    stat_pattern = re.compile(
        r"^(Parse|Execute|Fetch|total)\s+"
        r"(\d+)\s+"           # count
        r"([\d.]+)\s+"        # cpu
        r"([\d.]+)\s+"        # elapsed
        r"(\d+)\s+"           # disk
        r"(\d+)\s+"           # query
        r"(\d+)\s+"           # current
        r"(\d+)",             # rows
        re.IGNORECASE,
    )

    for i in range(stat_start, min(stat_start + 10, len(lines))):
        match = stat_pattern.match(lines[i].strip())
        if match:
            phase = match.group(1).lower()
            count = int(match.group(2))
            cpu = float(match.group(3))
            elapsed = float(match.group(4))
            disk = int(match.group(5))
            query = int(match.group(6))
            current = int(match.group(7))
            rows = int(match.group(8))

            if phase == "parse":
                stmt.parse_count, stmt.parse_cpu, stmt.parse_elapsed = count, cpu, elapsed
                stmt.parse_disk, stmt.parse_query, stmt.parse_current, stmt.parse_rows = disk, query, current, rows
            elif phase == "execute":
                stmt.execute_count, stmt.execute_cpu, stmt.execute_elapsed = count, cpu, elapsed
                stmt.execute_disk, stmt.execute_query, stmt.execute_current, stmt.execute_rows = disk, query, current, rows
            elif phase == "fetch":
                stmt.fetch_count, stmt.fetch_cpu, stmt.fetch_elapsed = count, cpu, elapsed
                stmt.fetch_disk, stmt.fetch_query, stmt.fetch_current, stmt.fetch_rows = disk, query, current, rows
            elif phase == "total":
                stmt.total_cpu, stmt.total_elapsed = cpu, elapsed
                stmt.total_disk, stmt.total_query, stmt.total_current, stmt.total_rows = disk, query, current, rows

    # 3. 실행계획 추출
    plan_start = -1
    for i in range(stat_start, len(lines)):
        if re.match(r"^Rows\s+(Row Source|Execution Plan)", lines[i], re.IGNORECASE):
            plan_start = i + 1
            break
        # 다른 형식의 실행계획 헤더
        if "Row Source Operation" in lines[i]:
            plan_start = i + 1
            break

    if plan_start > 0:
        for i in range(plan_start, len(lines)):
            line = lines[i].strip()
            if not line or line.startswith("Elapsed") or line.startswith("****"):
                break
            stmt.execution_plan.append(lines[i].rstrip())

    # 4. 대기 이벤트 추출
    wait_start = -1
    for i in range(stat_start, len(lines)):
        if re.match(r"^Elapsed times include waiting", lines[i], re.IGNORECASE):
            wait_start = i + 2  # 헤더 스킵
            break
        if re.match(r"^Event waited on", lines[i], re.IGNORECASE):
            wait_start = i + 2
            break

    if wait_start > 0:
        wait_pattern = re.compile(r"^(.+?)\s+(\d+)\s+([\d.]+)")
        for i in range(wait_start, len(lines)):
            line = lines[i].strip()
            if not line or line.startswith("****"):
                break
            match = wait_pattern.match(line)
            if match:
                stmt.wait_events.append({
                    "event": match.group(1).strip(),
                    "times_waited": int(match.group(2)),
                    "max_wait": float(match.group(3)),
                })

    # 5. 바인드 변수 파싱
    # tkprof level=4 이상일 때 BINDS 섹션 포함
    bind_section = False
    current_bind = {}
    for i, line in enumerate(lines):
        stripped = line.strip()
        # BINDS 섹션 시작
        if re.match(r"^BINDS\s*#", stripped):
            bind_section = True
            current_bind = {}
            continue
        if not bind_section:
            continue
        # 섹션 종료
        if stripped.startswith("****") or re.match(r"^(WAIT|STAT|CLOSE|EXEC|FETCH|PARSE)\s+#", stripped):
            if current_bind:
                stmt.bind_variables.append(current_bind)
                current_bind = {}
            bind_section = False
            continue
        # bind N: 새 바인드 시작
        bind_num = re.match(r"bind\s+(\d+):", stripped, re.IGNORECASE)
        if bind_num:
            if current_bind:
                stmt.bind_variables.append(current_bind)
            current_bind = {"position": int(bind_num.group(1)) + 1}
            continue
        # 값 파싱
        if current_bind is not None:
            kv = re.match(r"^\s*(\w+)=(.*)$", line)
            if kv:
                key, val = kv.group(1).lower(), kv.group(2).strip()
                if key == "value":
                    current_bind["value"] = val
                elif key == "dty":
                    # Oracle 타입 코드 → 이름
                    type_map = {
                        "1": "VARCHAR2", "2": "NUMBER", "12": "DATE",
                        "23": "RAW", "96": "CHAR", "112": "CLOB",
                        "113": "BLOB", "180": "TIMESTAMP", "181": "TIMESTAMP WITH TZ",
                    }
                    current_bind["type"] = type_map.get(val, f"TYPE({val})")
    if current_bind:
        stmt.bind_variables.append(current_bind)

    return stmt


def analyze_issues(stmt, config, logger):
    """
    실행계획 기반 개선 포인트 자동 태깅

    Returns:
        list[dict]: 발견된 이슈 목록
    """
    rules = config["tkprof"]["analysis_rules"]
    issues = []

    plan_text = "\n".join(stmt.execution_plan)

    # 1. Full Table Scan 감지
    fts_pattern = re.compile(r"(\d+)\s+TABLE ACCESS.*FULL\s+(\w+)", re.IGNORECASE)
    for match in fts_pattern.finditer(plan_text):
        rows = int(match.group(1))
        table_name = match.group(2)
        if rows >= rules.get("full_table_scan_threshold", 10000):
            issues.append({
                "type": "FULL_TABLE_SCAN",
                "severity": "HIGH" if rows > 100000 else "MEDIUM",
                "table": table_name,
                "rows": rows,
                "message": f"테이블 {table_name}에 대한 Full Table Scan ({rows:,}행). "
                          f"인덱스 생성 또는 WHERE 조건 검토 필요.",
            })

    # 2. Nested Loop 조인 고비용 감지
    nl_pattern = re.compile(r"(\d+)\s+NESTED LOOPS", re.IGNORECASE)
    for match in nl_pattern.finditer(plan_text):
        rows = int(match.group(1))
        if rows >= rules.get("nested_loop_threshold", 100000):
            issues.append({
                "type": "HIGH_COST_NL_JOIN",
                "severity": "HIGH",
                "rows": rows,
                "message": f"Nested Loop 조인에서 {rows:,}행 처리. "
                          f"Hash Join 또는 인덱스 조정 검토 필요.",
            })

    # 3. 행당 Buffer Gets 과다
    if stmt.fetch_rows > 0:
        buf_per_row = (stmt.total_query + stmt.total_current) / stmt.fetch_rows
        threshold = rules.get("buffer_gets_per_row_threshold", 100)
        if buf_per_row > threshold:
            issues.append({
                "type": "HIGH_BUFFER_GETS_PER_ROW",
                "severity": "MEDIUM",
                "buffer_gets_per_row": round(buf_per_row, 1),
                "message": f"행당 Buffer Gets가 {buf_per_row:.1f}회로 과다. "
                          f"실행계획 및 인덱스 효율 검토 필요.",
            })

    # 4. 과도한 Parse 횟수 (하드 파싱 의심)
    if stmt.parse_count > 1 and stmt.execute_count > 0:
        parse_ratio = stmt.parse_count / stmt.execute_count
        if parse_ratio > 0.5:
            issues.append({
                "type": "EXCESSIVE_PARSING",
                "severity": "MEDIUM",
                "parse_ratio": round(parse_ratio, 2),
                "message": f"Parse/Execute 비율이 {parse_ratio:.2f}로 높음. "
                          f"바인드 변수 사용 또는 커서 캐싱 검토.",
            })

    # 5. 디스크 읽기 비율 높음
    total_gets = stmt.total_query + stmt.total_current
    if total_gets > 0 and stmt.total_disk > 0:
        disk_ratio = stmt.total_disk / total_gets
        if disk_ratio > 0.3:
            issues.append({
                "type": "HIGH_DISK_READ_RATIO",
                "severity": "LOW" if disk_ratio < 0.5 else "MEDIUM",
                "disk_ratio": round(disk_ratio, 2),
                "message": f"디스크 읽기 비율 {disk_ratio:.1%}. "
                          f"Buffer Cache 적중률 확인 및 자주 사용하는 데이터 캐싱 검토.",
            })

    # 6. 대기 이벤트 분석
    for wait in stmt.wait_events:
        event = wait["event"]
        if "db file sequential read" in event.lower() and wait["times_waited"] > 1000:
            issues.append({
                "type": "EXCESSIVE_INDEX_READ",
                "severity": "MEDIUM",
                "event": event,
                "times_waited": wait["times_waited"],
                "message": f"인덱스 읽기 대기 {wait['times_waited']:,}회. "
                          f"인덱스 클러스터링 팩터 확인 필요.",
            })
        elif "db file scattered read" in event.lower() and wait["times_waited"] > 100:
            issues.append({
                "type": "EXCESSIVE_FTS_READ",
                "severity": "HIGH",
                "event": event,
                "times_waited": wait["times_waited"],
                "message": f"Full Scan 읽기 대기 {wait['times_waited']:,}회. "
                          f"인덱스 부재 또는 비효율적 쿼리 의심.",
            })

    stmt.issues = issues
    return issues


# ============================================
# HTML 리포트 생성
# ============================================
# AWR JSON 리포트 생성
# ============================================
def process_awr_json(json_file, config, logger):
    """AWR JSON 파일을 읽어 HTML 리포트 생성"""
    import json as json_mod

    with open(json_file, "r", encoding="utf-8") as f:
        data = json_mod.load(f)

    sql_id = data.get("sql_id", "unknown")
    sql_text = data.get("sql_text", "")
    plan_lines = data.get("plan", [])
    stats = data.get("stats", [])

    logger.info(f"  AWR 데이터 로드: sql_id={sql_id}, plan={len(plan_lines)}줄, stats={len(stats)}개")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    file_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    report_dir = Path(config["paths"]["report_output"])
    report_dir.mkdir(parents=True, exist_ok=True)
    report_file = report_dir / f"awr_{sql_id}_{file_ts}.html"

    # 통계 테이블 HTML
    stats_html = ""
    if stats:
        rows_html = ""
        for s in stats:
            rows_html += f"""<tr>
                <td>{s.get('snap_time','')}</td>
                <td>{s.get('executions_delta',0):,}</td>
                <td>{s.get('avg_elapsed_sec','')}</td>
                <td>{s.get('avg_cpu_sec','')}</td>
                <td>{s.get('avg_buffer_gets',''):,}</td>
                <td>{s.get('avg_disk_reads',''):,}</td>
                <td>{s.get('avg_rows',''):,}</td>
            </tr>"""
        stats_html = f"""
        <h3>📊 AWR 성능 통계 (최근 스냅샷)</h3>
        <table class="stats-table">
          <tr><th>스냅샷 시간</th><th>실행횟수</th><th>평균경과(s)</th><th>평균CPU(s)</th><th>평균Buffer Gets</th><th>평균Disk Reads</th><th>평균Rows</th></tr>
          {rows_html}
        </table>"""

    # 실행계획 HTML
    plan_html = ""
    if plan_lines:
        plan_text = "\n".join(plan_lines)
        plan_html = f"""
        <h3>📋 실행계획 (AWR)</h3>
        <div class="plan">{plan_text}</div>"""

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<title>AWR 분석 리포트 - {sql_id}</title>
<style>
  body {{ font-family: 'Malgun Gothic', sans-serif; margin: 20px; background: #f5f5f5; color: #333; }}
  .container {{ max-width: 1200px; margin: 0 auto; }}
  .header {{ background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
  .header h1 {{ margin: 0 0 8px 0; font-size: 22px; }}
  .header .meta {{ font-size: 13px; opacity: 0.8; }}
  .card {{ background: white; border-radius: 8px; padding: 20px; margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  .sql-text {{ background: #f8f9fa; padding: 12px; border-radius: 4px; font-family: 'Consolas', monospace; font-size: 13px; white-space: pre-wrap; word-break: break-all; max-height: 200px; overflow-y: auto; }}
  .plan {{ background: #1e1e1e; color: #d4d4d4; padding: 12px; border-radius: 4px; font-family: 'Consolas', monospace; font-size: 12px; white-space: pre; overflow-x: auto; }}
  .stats-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  .stats-table th {{ background: #ecf0f1; padding: 8px; text-align: right; border: 1px solid #ddd; }}
  .stats-table td {{ padding: 8px; text-align: right; border: 1px solid #ddd; }}
  .stats-table th:first-child, .stats-table td:first-child {{ text-align: left; }}
  h3 {{ color: #2c3e50; margin-top: 20px; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>AWR SQL 분석 리포트</h1>
    <div class="meta">SQL_ID: {sql_id} | 생성: {timestamp}</div>
  </div>
  <div class="card">
    <h3>🔍 SQL 텍스트</h3>
    <div class="sql-text">{sql_text}</div>
    {plan_html}
    {stats_html}
  </div>
</div>
</body>
</html>"""

    with open(report_file, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info(f"  AWR HTML 리포트 생성: {report_file}")
    return {
        "trc_file": json_file,
        "tkprof_file": None,
        "report_file": str(report_file),
        "sql_count": 1,
        "issue_count": 0,
    }


# ============================================
def generate_html_report(statements, trc_file, output_path, logger):
    """분석 결과를 HTML 리포트로 생성"""

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    trc_name = Path(trc_file).stem

    severity_colors = {
        "HIGH": "#E24B4A",
        "MEDIUM": "#EF9F27",
        "LOW": "#639922",
    }

    html_parts = [f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<title>SQL 튜닝 분석 리포트 - {trc_name}</title>
<style>
  body {{ font-family: 'Malgun Gothic', sans-serif; margin: 20px; background: #f5f5f5; color: #333; }}
  .container {{ max-width: 1200px; margin: 0 auto; }}
  .header {{ background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
  .header h1 {{ margin: 0 0 8px 0; font-size: 22px; }}
  .header .meta {{ font-size: 13px; opacity: 0.8; }}
  .summary {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }}
  .summary-card {{ background: white; padding: 16px; border-radius: 8px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  .summary-card .value {{ font-size: 28px; font-weight: bold; color: #2c3e50; }}
  .summary-card .label {{ font-size: 12px; color: #888; margin-top: 4px; }}
  .sql-block {{ background: white; border-radius: 8px; margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); overflow: hidden; }}
  .sql-header {{ background: #34495e; color: white; padding: 12px 16px; display: flex; justify-content: space-between; align-items: center; }}
  .sql-header h3 {{ margin: 0; font-size: 15px; }}
  .sql-body {{ padding: 16px; }}
  .sql-text {{ background: #f8f9fa; padding: 12px; border-radius: 4px; font-family: 'Consolas', monospace; font-size: 13px; white-space: pre-wrap; word-break: break-all; max-height: 200px; overflow-y: auto; margin-bottom: 12px; }}
  .stats-table {{ width: 100%; border-collapse: collapse; font-size: 13px; margin-bottom: 12px; }}
  .stats-table th {{ background: #ecf0f1; padding: 8px; text-align: right; border: 1px solid #ddd; }}
  .stats-table td {{ padding: 8px; text-align: right; border: 1px solid #ddd; }}
  .stats-table td:first-child, .stats-table th:first-child {{ text-align: left; font-weight: bold; }}
  .plan {{ background: #1e1e1e; color: #d4d4d4; padding: 12px; border-radius: 4px; font-family: 'Consolas', monospace; font-size: 12px; white-space: pre; overflow-x: auto; margin-bottom: 12px; }}
  .issue {{ padding: 10px 14px; border-radius: 4px; margin-bottom: 8px; border-left: 4px solid; }}
  .issue-HIGH {{ background: #fdf0f0; border-color: #E24B4A; }}
  .issue-MEDIUM {{ background: #fef8ed; border-color: #EF9F27; }}
  .issue-LOW {{ background: #f0f7e8; border-color: #639922; }}
  .issue .badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: bold; color: white; margin-right: 8px; }}
  .wait-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  .wait-table th, .wait-table td {{ padding: 6px 10px; border: 1px solid #ddd; }}
  .wait-table th {{ background: #ecf0f1; text-align: left; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>SQL 튜닝 분석 리포트</h1>
    <div class="meta">트레이스: {trc_name} | 생성: {timestamp}</div>
  </div>
"""]

    # 요약
    total_issues = sum(len(s.issues) for s in statements)
    high_issues = sum(1 for s in statements for i in s.issues if i["severity"] == "HIGH")
    total_elapsed = sum(s.total_elapsed for s in statements)

    html_parts.append(f"""
  <div class="summary">
    <div class="summary-card"><div class="value">{len(statements)}</div><div class="label">분석 SQL 수</div></div>
    <div class="summary-card"><div class="value">{total_issues}</div><div class="label">발견된 이슈</div></div>
    <div class="summary-card"><div class="value" style="color:#E24B4A">{high_issues}</div><div class="label">HIGH 이슈</div></div>
    <div class="summary-card"><div class="value">{total_elapsed:.2f}s</div><div class="label">총 경과시간</div></div>
  </div>
""")

    # SQL별 상세
    for idx, stmt in enumerate(statements, 1):
        issue_count = len(stmt.issues)
        severity_badge = ""
        if issue_count > 0:
            max_severity = max(stmt.issues, key=lambda x: {"HIGH": 3, "MEDIUM": 2, "LOW": 1}.get(x["severity"], 0))
            color = severity_colors.get(max_severity["severity"], "#888")
            severity_badge = f'<span style="background:{color};color:white;padding:2px 10px;border-radius:10px;font-size:12px">{issue_count}건</span>'

        sql_display = stmt.sql_text[:500] + ("..." if len(stmt.sql_text) > 500 else "")

        html_parts.append(f"""
  <div class="sql-block">
    <div class="sql-header">
      <h3>#{idx} {stmt.sql_id or 'N/A'}</h3>
      {severity_badge}
    </div>
    <div class="sql-body">
      <div class="sql-text">{sql_display}</div>

      <table class="stats-table">
        <tr><th>Phase</th><th>Count</th><th>CPU(s)</th><th>Elapsed(s)</th><th>Disk</th><th>Query</th><th>Current</th><th>Rows</th></tr>
        <tr><td>Parse</td><td>{stmt.parse_count}</td><td>{stmt.parse_cpu:.3f}</td><td>{stmt.parse_elapsed:.3f}</td><td>{stmt.parse_disk:,}</td><td>{stmt.parse_query:,}</td><td>{stmt.parse_current:,}</td><td>{stmt.parse_rows:,}</td></tr>
        <tr><td>Execute</td><td>{stmt.execute_count}</td><td>{stmt.execute_cpu:.3f}</td><td>{stmt.execute_elapsed:.3f}</td><td>{stmt.execute_disk:,}</td><td>{stmt.execute_query:,}</td><td>{stmt.execute_current:,}</td><td>{stmt.execute_rows:,}</td></tr>
        <tr><td>Fetch</td><td>{stmt.fetch_count}</td><td>{stmt.fetch_cpu:.3f}</td><td>{stmt.fetch_elapsed:.3f}</td><td>{stmt.fetch_disk:,}</td><td>{stmt.fetch_query:,}</td><td>{stmt.fetch_current:,}</td><td>{stmt.fetch_rows:,}</td></tr>
        <tr style="font-weight:bold"><td>Total</td><td></td><td>{stmt.total_cpu:.3f}</td><td>{stmt.total_elapsed:.3f}</td><td>{stmt.total_disk:,}</td><td>{stmt.total_query:,}</td><td>{stmt.total_current:,}</td><td>{stmt.total_rows:,}</td></tr>
      </table>
""")

        # 실행계획
        if stmt.execution_plan:
            plan_text = "\n".join(stmt.execution_plan)
            html_parts.append(f"""
      <h4>실행계획 (Row Source Operation)</h4>
      <div class="plan">{plan_text}</div>
""")

        # 대기 이벤트
        if stmt.wait_events:
            html_parts.append("""
      <h4>대기 이벤트</h4>
      <table class="wait-table">
        <tr><th>Event</th><th>대기횟수</th><th>최대대기(s)</th></tr>
""")
            for w in stmt.wait_events:
                html_parts.append(
                    f"        <tr><td>{w['event']}</td>"
                    f"<td>{w['times_waited']:,}</td>"
                    f"<td>{w['max_wait']:.4f}</td></tr>\n"
                )
            html_parts.append("      </table>\n")

        # 이슈
        if stmt.issues:
            html_parts.append("      <h4>개선 포인트</h4>\n")
            for issue in stmt.issues:
                sev = issue["severity"]
                color = severity_colors.get(sev, "#888")
                html_parts.append(f"""
      <div class="issue issue-{sev}">
        <span class="badge" style="background:{color}">{sev}</span>
        <strong>[{issue['type']}]</strong> {issue['message']}
      </div>
""")

        html_parts.append("    </div>\n  </div>\n")

    html_parts.append("</div>\n</body>\n</html>")

    # 파일 저장
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("".join(html_parts))

    logger.info(f"  HTML 리포트 생성: {output_path}")
    return output_path


# ============================================
# 메인 처리
# ============================================
def process_trace_file(trc_file, config, logger):
    """
    단일 트레이스 파일 처리 (tkprof → 파싱 → 분석 → 리포트)
    AWR JSON 파일인 경우 별도 처리
    """
    trc_path = Path(trc_file)
    trc_name = trc_path.stem

    logger.info(f"\n{'─' * 60}")
    logger.info(f"Processing: {trc_path.name}")

    # AWR JSON 파일 처리
    if trc_path.suffix == ".json" and "_awr_" in trc_path.name:
        return process_awr_json(trc_file, config, logger)

    # tkprof 출력 경로
    tkprof_dir = Path(config["paths"]["tkprof_output"])
    tkprof_dir.mkdir(parents=True, exist_ok=True)
    tkprof_file = tkprof_dir / f"{trc_name}.prf"

    # 1. tkprof 실행
    result = run_tkprof(str(trc_file), str(tkprof_file), config, logger)
    if not result:
        return None

    # 2. 파싱
    statements, tkprof_full_text = parse_tkprof_output(str(tkprof_file), logger)

    # 3. 이슈 분석
    total_issues = 0
    for stmt in statements:
        issues = analyze_issues(stmt, config, logger)
        total_issues += len(issues)

    logger.info(f"  총 이슈: {total_issues}건")

    # 4. HTML 리포트 생성
    report_dir = Path(config["paths"]["report_output"])
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = report_dir / f"tkprof_{trc_name}_{timestamp}.html"

    generate_html_report(statements, str(trc_file), str(report_file), logger)

    # 5. 엑셀용 JSON 저장 (AWR JSON과 동일한 디렉토리)
    trace_dir = Path(config["paths"]["trace_output"])
    json_path = trace_dir / f"{trc_name}_tkprof_{timestamp}.json"
    import json as json_mod
    json_data = {
        "sql_id": trc_name.split("_")[0] if statements else trc_name,
        "source": "tkprof",
        "trc_file": str(trc_file),
        "tkprof_file": str(tkprof_file),
        "tkprof_full_text": tkprof_full_text,
        "plan": [],
        "stats": [],
        "sql_text": statements[0].sql_text if statements else "",
        "tkprof_statements": [
            {
                "sql_id":         s.sql_id,
                "sql_text":       s.sql_text,
                "parse_count":    s.parse_count,    "parse_cpu":     s.parse_cpu,
                "parse_elapsed":  s.parse_elapsed,  "parse_disk":    s.parse_disk,
                "parse_query":    s.parse_query,    "parse_rows":    s.parse_rows,
                "execute_count":  s.execute_count,  "execute_cpu":   s.execute_cpu,
                "execute_elapsed":s.execute_elapsed,"execute_disk":  s.execute_disk,
                "execute_query":  s.execute_query,  "execute_rows":  s.execute_rows,
                "fetch_count":    s.fetch_count,    "fetch_cpu":     s.fetch_cpu,
                "fetch_elapsed":  s.fetch_elapsed,  "fetch_disk":    s.fetch_disk,
                "fetch_query":    s.fetch_query,    "fetch_rows":    s.fetch_rows,
                "total_cpu":      s.total_cpu,      "total_elapsed": s.total_elapsed,
                "total_disk":     s.total_disk,     "total_query":   s.total_query,
                "total_rows":     s.total_rows,
                "execution_plan": s.execution_plan,
                "wait_events":    s.wait_events,
                "bind_variables": s.bind_variables,
                "issues":         s.issues,
            }
            for s in statements
        ]
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json_mod.dump(json_data, f, ensure_ascii=False, indent=2, default=str)
    logger.info(f"  tkprof JSON 저장: {json_path.name}")

    return {
        "trc_file":    str(trc_file),
        "tkprof_file": str(tkprof_file),
        "json_file":   str(json_path),
        "report_file": str(report_file),
        "sql_count":   len(statements),
        "issue_count": total_issues,
    }


def main():
    parser = argparse.ArgumentParser(description="Phase 3: tkprof 분석 리포트")
    parser.add_argument("trace_path", help="트레이스 파일 또는 디렉토리 경로")
    parser.add_argument("--config", type=str, help="설정 파일 경로")
    args = parser.parse_args()

    config = load_config(args.config)
    logger = setup_logger("phase3_tkprof", config)

    trace_path = Path(args.trace_path)

    try:
        if trace_path.is_dir():
            # 디렉토리 내 모든 .trc 파일 처리
            trc_files = sorted(trace_path.glob("*.trc"))
            logger.info(f"Phase 3: {len(trc_files)}개 트레이스 파일 처리")

            results = []
            for trc in trc_files:
                result = process_trace_file(str(trc), config, logger)
                if result:
                    results.append(result)

            logger.info(f"\nPhase 3 완료: {len(results)}/{len(trc_files)}건 처리")

        elif trace_path.is_file():
            result = process_trace_file(str(trace_path), config, logger)
            if result:
                logger.info(f"Phase 3 완료: 리포트 → {result['report_file']}")

        else:
            logger.error(f"경로를 찾을 수 없음: {trace_path}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Phase 3 오류: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
