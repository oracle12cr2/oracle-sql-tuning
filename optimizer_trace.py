#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
10053 Optimizer Trace 수집 및 분석 모듈
Oracle 옵티마이저가 실행계획을 선택하는 과정을 추적하고 분석
"""

import os
import re
import time
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import oracledb

from trace_collector import SSHClient
from utils import get_oracle_connection, load_config


class OptimizerTraceCollector:
    """10053 옵티마이저 트레이스 수집기"""
    
    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger
        self.db_config = config['database']
        self.optimizer_config = config.get('optimizer_trace', {})
        self.ssh_client = None
        
    def collect_10053(self, sql_id: str, sql_text: str = None) -> Optional[str]:
        """
        특정 SQL_ID에 대해 10053 트레이스 수집
        
        Args:
            sql_id: Oracle SQL_ID
            sql_text: SQL 텍스트 (EXPLAIN PLAN용)
            
        Returns:
            수집된 트레이스 파일 경로 (로컬)
        """
        self.logger.info(f"10053 트레이스 수집 시작: SQL_ID={sql_id}")
        
        try:
            # Oracle DB 연결
            conn = get_oracle_connection(self.config)
            cursor = conn.cursor()
            
            # 트레이스 파일명 생성용 세션 정보
            cursor.execute("SELECT SID, SERIAL# FROM V$SESSION WHERE AUDSID = USERENV('SESSIONID')")
            sid, serial = cursor.fetchone()
            
            # 트레이스 파일명 예상
            trace_file_pattern = f"*{sid}*.trc"
            
            # 현재 세션의 정확한 트레이스 파일 경로 조회
            cursor.execute("SELECT VALUE FROM V$DIAG_INFO WHERE NAME = 'Default Trace File'")
            trace_row = cursor.fetchone()
            self._exact_trace_file = trace_row[0] if trace_row else None
            if self._exact_trace_file:
                self.logger.info(f"  트레이스 파일: {self._exact_trace_file}")
            
            # 1. 10053 트레이스 활성화
            self.logger.info("10053 트레이스 활성화")
            cursor.execute("ALTER SESSION SET EVENTS '10053 trace name context forever, level 1'")
            
            # 2. SQL 실행 (EXPLAIN PLAN 또는 실제 실행)
            collection_method = self.optimizer_config.get('collection_method', 'explain')
            
            if collection_method == 'explain':
                # EXPLAIN PLAN 방식
                if sql_text:
                    # EXPLAIN PLAN FOR 중복 방지
                    clean_sql = sql_text.strip()
                    if clean_sql.upper().startswith("EXPLAIN PLAN"):
                        clean_sql = clean_sql[len("EXPLAIN PLAN FOR"):].strip()
                    self.logger.info("EXPLAIN PLAN 실행")
                    try:
                        cursor.execute(f"EXPLAIN PLAN FOR {clean_sql}")
                    except Exception as explain_err:
                        if 'bind variable' in str(explain_err).lower() or 'DPY-4010' in str(explain_err):
                            self.logger.info(f"  바인드 변수 감지 → 리터럴 치환 후 재시도")
                            clean_sql = self._replace_binds_with_literals(sql_id, clean_sql, cursor)
                            cursor.execute(f"EXPLAIN PLAN FOR {clean_sql}")
                        else:
                            raise
                else:
                    # SQL_ID로부터 SQL 텍스트 조회
                    cursor.execute("""
                        SELECT SQL_FULLTEXT 
                        FROM V$SQL 
                        WHERE SQL_ID = :sql_id 
                        AND ROWNUM = 1
                    """, sql_id=sql_id)
                    result = cursor.fetchone()
                    if result:
                        sql_text = result[0]
                        clean = sql_text.strip()
                        if clean.upper().startswith("EXPLAIN PLAN"):
                            clean = clean[len("EXPLAIN PLAN FOR"):].strip()
                        try:
                            cursor.execute(f"EXPLAIN PLAN FOR {clean}")
                        except Exception as explain_err:
                            if 'bind variable' in str(explain_err).lower() or 'DPY-4010' in str(explain_err):
                                self.logger.info(f"  바인드 변수 감지 → 리터럴 치환 후 재시도")
                                clean = self._replace_binds_with_literals(sql_id, clean, cursor)
                                cursor.execute(f"EXPLAIN PLAN FOR {clean}")
                            else:
                                raise
                    else:
                        raise ValueError(f"SQL_ID {sql_id}에 대한 SQL을 찾을 수 없음")
            else:
                # 실제 실행 방식 (주의: 운영 환경에서는 위험할 수 있음)
                self.logger.warning("실제 SQL 실행 모드 (주의)")
                if sql_text:
                    cursor.execute(sql_text)
                else:
                    raise ValueError("실제 실행 모드에서는 sql_text가 필요함")
            
            # 잠시 대기 (트레이스 완료를 위해)
            time.sleep(1)
            
            # 3. 10053 트레이스 비활성화
            self.logger.info("10053 트레이스 비활성화")
            cursor.execute("ALTER SESSION SET EVENTS '10053 trace name context off'")
            
            # 4. 트레이스 파일 수집
            # 정확한 트레이스 파일 경로가 있으면 직접 수집
            if hasattr(self, '_exact_trace_file') and self._exact_trace_file:
                local_trace_file = self._collect_exact_trace_file(self._exact_trace_file, sql_id)
            else:
                local_trace_file = self._collect_trace_file(trace_file_pattern, sql_id)
            
            cursor.close()
            conn.close()
            
            if local_trace_file:
                self.logger.info(f"10053 트레이스 수집 완료: {local_trace_file}")
                return local_trace_file
            else:
                self.logger.error("트레이스 파일을 찾을 수 없음")
                return None
                
        except Exception as e:
            self.logger.error(f"10053 트레이스 수집 실패: {e}")
            return None
    

    def _replace_binds_with_literals(self, sql_id: str, sql_text: str, cursor) -> str:
        """
        바인드 변수를 V$SQL_BIND_CAPTURE의 실제 값으로 치환
        EXPLAIN PLAN은 바인드 변수를 처리할 수 없으므로 리터럴로 변환
        """
        try:
            cursor.execute("""
                SELECT NAME, DATATYPE_STRING, VALUE_STRING
                FROM V$SQL_BIND_CAPTURE
                WHERE SQL_ID = :sql_id
                ORDER BY POSITION
            """, sql_id=sql_id)
            binds = cursor.fetchall()
            
            if not binds:
                # 바인드 캡처가 없으면 기본값으로 치환
                # :xxx 형태를 'DUMMY'로 치환 (숫자형은 0)
                result = re.sub(r':(\w+)', "'DUMMY'", sql_text)
                self.logger.info(f"  바인드 캡처 없음 → 기본값으로 치환 ({len(re.findall(r':' + chr(92) + 'w+', sql_text))}개)")
                return result
            
            result = sql_text
            for name, datatype, value in binds:
                if not name:
                    continue
                bind_name = name if name.startswith(':') else ':' + name
                
                if value is None:
                    replacement = "NULL"
                elif datatype and ('CHAR' in datatype.upper() or 'VARCHAR' in datatype.upper() or 'DATE' in datatype.upper()):
                    replacement = f"'{value}'"
                elif datatype and 'NUMBER' in datatype.upper():
                    replacement = str(value)
                else:
                    replacement = f"'{value}'"
                
                # 대소문자 무시 치환
                pattern = re.compile(re.escape(bind_name), re.IGNORECASE)
                result = pattern.sub(replacement, result)
            
            # 아직 남은 바인드 변수 처리
            remaining = re.findall(r':\w+', result)
            if remaining:
                self.logger.info(f"  미처리 바인드 {len(remaining)}개 → 기본값 치환")
                result = re.sub(r':(\w+)', "'DUMMY'", result)
            
            self.logger.info(f"  바인드 변수 {len(binds)}개 리터럴 치환 완료")
            return result
            
        except Exception as e:
            self.logger.warning(f"  바인드 치환 실패: {e} → 기본값 사용")
            return re.sub(r':(\w+)', "'DUMMY'", sql_text)


    def _collect_exact_trace_file(self, remote_trace_path: str, sql_id: str) -> Optional[str]:
        """정확한 경로의 트레이스 파일을 SSH로 수집"""
        try:
            if not self.ssh_client:
                self.ssh_client = SSHClient(self.config, self.logger)
            
            # 파일 존재 확인
            check_cmd = f"test -f {remote_trace_path} && echo EXISTS || echo NOTFOUND"
            result = self.ssh_client.run_remote_cmd(check_cmd).strip()
            
            if result != "EXISTS":
                self.logger.warning(f"트레이스 파일 없음: {remote_trace_path}")
                return None
            
            # 10053 내용 확인
            check_10053 = f"grep -c 'SINGLE TABLE ACCESS PATH\\|PARAMETERS USED BY THE OPTIMIZER' {remote_trace_path} 2>/dev/null"
            count = self.ssh_client.run_remote_cmd(check_10053).strip()
            self.logger.info(f"  10053 키워드: {count}건")
            
            # 로컬로 복사
            local_dir = os.path.join(
                self.config.get('paths', {}).get('trace_output', 'output/traces')
            )
            os.makedirs(local_dir, exist_ok=True)
            local_file = os.path.join(local_dir, f"10053_{sql_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.trc")
            
            # SCP 또는 cat으로 복사
            content = self.ssh_client.run_remote_cmd(f"cat {remote_trace_path}")
            with open(local_file, 'w', encoding='utf-8', errors='replace') as f:
                f.write(content)
            
            file_size = os.path.getsize(local_file)
            self.logger.info(f"  트레이스 수집 완료: {local_file} ({file_size:,} bytes)")
            return local_file
            
        except Exception as e:
            self.logger.error(f"  트레이스 파일 수집 실패: {e}")
            return None

    def _collect_trace_file(self, trace_file_pattern: str, sql_id: str) -> Optional[str]:
        """SSH를 통해 원격 서버에서 트레이스 파일 수집"""
        try:
            # SSH 클라이언트 초기화
            if not self.ssh_client:
                self.ssh_client = SSHClient(self.config, self.logger)
            
            # 트레이스 디렉터리에서 최신 파일 찾기
            trace_dir = self.db_config.get('trace_directory', '/u01/app/oracle/diag/rdbms/prod/prod/trace')
            
            # 최신 trc 파일 찾기 (10053 내용이 있는 것)
            find_cmd = f"""
            find {trace_dir} -name "{trace_file_pattern}" -newer /tmp/trace_start_time -exec grep -l "SINGLE TABLE ACCESS PATH\\|JOIN ORDER" {{}} \\; 2>/dev/null | head -1
            """
            
            # 시작 시간 마커 생성
            self.ssh_client.run_remote_cmd(f"touch /tmp/trace_start_time")
            
            # 트레이스 파일 찾기
            trace_files = self.ssh_client.run_remote_cmd(find_cmd).strip().split('\n')
            trace_files = [f for f in trace_files if f.strip()]
            
            if not trace_files:
                self.logger.warning("10053 내용이 포함된 트레이스 파일을 찾을 수 없음")
                return None
            
            remote_trace_file = trace_files[0]
            self.logger.info(f"원격 트레이스 파일 발견: {remote_trace_file}")
            
            # 로컬 저장 경로
            local_dir = os.path.join(self.config['output']['base_directory'], 'traces')
            os.makedirs(local_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            local_trace_file = os.path.join(local_dir, f"10053_{sql_id}_{timestamp}.trc")
            
            # 파일 다운로드
            if self.ssh_client.download_file(remote_trace_file, local_trace_file):
                return local_trace_file
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"트레이스 파일 수집 실패: {e}")
            return None


class OptimizerTraceAnalyzer:
    """10053 트레이스 분석기"""
    
    def __init__(self, config: Dict[str, Any] = None, logger=None):
        self.config = config or {}
        self.logger = logger
        self.optimizer_config = self.config.get('optimizer_trace', {})
    
    def _log(self, level: str, msg: str):
        if self.logger:
            getattr(self.logger, level)(msg)
    
    def parse_10053(self, trace_file_path: str) -> Dict[str, Any]:
        """
        10053 트레이스 파일 파싱
        
        Args:
            trace_file_path: 트레이스 파일 경로
            
        Returns:
            파싱된 데이터 딕셔너리
        """
        self._log('info', f"10053 트레이스 분석 시작: {trace_file_path}")
        
        parsed_data = {
            'file_path': trace_file_path,
            'parse_time': datetime.now(),
            'sql_text': '',
            'sql_id': '',
            'db_info': {},
            'system_statistics': {},
            'optimizer_parameters_altered': {},
            'optimizer_parameters_default': {},
            'base_statistics': {},
            'index_statistics': {},
            'column_statistics': {},
            'table_access_paths': [],
            'join_orders': [],
            'best_join_order': {},
            'join_permutations_tried': 0,
            'cost_analysis': {},
            'dynamic_sampling': [],
            'query_transformations': [],
            'issues': [],
            'is_single_table': False,
        }
        
        try:
            with open(trace_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # 0. DB 정보 및 SQL 텍스트 추출
            self._extract_db_info(content, parsed_data)
            self._extract_sql_text(content, parsed_data)
            
            # 1. 시스템 통계 추출
            self._extract_system_statistics(content, parsed_data)
            
            # 2. 옵티마이저 파라미터 추출 (Altered / Default 분리)
            self._extract_optimizer_parameters(content, parsed_data)
            
            # 3. 기본 통계 정보 추출
            self._extract_base_statistics(content, parsed_data)
            
            # 4. 테이블 접근 경로 분석
            self._extract_table_access_paths(content, parsed_data)
            
            # 5. 조인 순서 분석
            self._extract_join_orders(content, parsed_data)
            
            # 6. 비용 분석
            self._analyze_costs(parsed_data)

            # 6.5 동적 샘플링 감지
            self._extract_dynamic_sampling(content, parsed_data)

            # 6.6 쿼리 변환 추출
            self._extract_query_transformations(content, parsed_data)

            # 7. 이슈 감지
            self._detect_issues(parsed_data)
            
            self._log('info', "10053 트레이스 분석 완료")
            return parsed_data
            
        except Exception as e:
            self._log('error', f"10053 트레이스 분석 실패: {e}")
            return parsed_data
    
    def _extract_db_info(self, content: str, parsed_data: Dict[str, Any]):
        """DB 정보 추출"""
        info = {}
        for key, pattern in [
            ('oracle_version', r'Oracle Database (.+?)$'),
            ('instance_name', r'Instance name:\s*(\S+)'),
            ('database_name', r'Database name:\s*(\S+)'),
            ('database_role', r'Database role:\s*(\S+)'),
            ('node_name', r'Node name:\s*(\S+)'),
        ]:
            m = re.search(pattern, content, re.MULTILINE)
            if m:
                info[key] = m.group(1).strip()
        parsed_data['db_info'] = info
    
    def _extract_sql_text(self, content: str, parsed_data: Dict[str, Any]):
        """SQL 텍스트 및 SQL_ID 추출"""
        # Current SQL Statement for this session (sql_id=xxx)
        sql_m = re.search(
            r'----- Current SQL Statement for this session \(sql_id=(\w+)\) -----\n(.*?)(?=----- |\n\*)',
            content, re.DOTALL
        )
        if sql_m:
            parsed_data['sql_id'] = sql_m.group(1)
            parsed_data['sql_text'] = sql_m.group(2).strip()
    
    def _extract_system_statistics(self, content: str, parsed_data: Dict[str, Any]):
        """시스템 통계 추출 (CPUSPEED, SREADTIM, MREADTIM, MBRC, IOSEEKTIM 등)"""
        sys_section = re.search(
            r'SYSTEM STATISTICS INFORMATION\s*[-]+\s*(.*?)(?=\n\*{5,})',
            content, re.DOTALL
        )
        if not sys_section:
            return
            
        text = sys_section.group(1)
        stats = {}
        
        # Using NOWORKLOAD / WORKLOAD stats
        workload_m = re.search(r'Using\s+(NOWORKLOAD|WORKLOAD)\s+Stats', text)
        if workload_m:
            stats['stats_type'] = workload_m.group(1)
        
        # Extract each stat: CPUSPEEDNW: 3107 millions instructions/sec (default is 100)
        stat_pattern = re.compile(
            r'(\w+):\s+(\S+)\s+(.*?)\(default is (\S+)\)'
        )
        for m in stat_pattern.finditer(text):
            name = m.group(1)
            value = m.group(2)
            unit = m.group(3).strip()
            default = m.group(4)
            stats[name] = {
                'value': value,
                'unit': unit,
                'default': default,
                'is_default': value == default or value == 'NO' 
            }
        
        parsed_data['system_statistics'] = stats

    def _extract_optimizer_parameters(self, content: str, parsed_data: Dict[str, Any]):
        """옵티마이저 파라미터 추출 — Altered / Default 분리"""
        param_line_re = re.compile(r'^\s*(\S+)\s+=\s+(.+)$', re.MULTILINE)
        
        # PARAMETERS WITH ALTERED VALUES 섹션
        altered_m = re.search(
            r'PARAMETERS WITH ALTERED VALUES\s*\n\s*\*+\s*\nCompilation Environment Dump\n(.*?)(?=Bug Fix Control Environment|\*{5,}\s*\n\s*PARAMETERS WITH DEFAULT)',
            content, re.DOTALL
        )
        if altered_m:
            for m in param_line_re.finditer(altered_m.group(1)):
                parsed_data['optimizer_parameters_altered'][m.group(1)] = m.group(2).strip()
        
        # PARAMETERS WITH DEFAULT VALUES 섹션 (첫 번째 블록만)
        default_m = re.search(
            r'PARAMETERS WITH DEFAULT VALUES\s*\n\s*\*+\s*\nCompilation Environment Dump\n(.*?)(?=\n\s*Table:|\nSYSTEM STATISTICS)',
            content, re.DOTALL
        )
        if default_m:
            for m in param_line_re.finditer(default_m.group(1)):
                parsed_data['optimizer_parameters_default'][m.group(1)] = m.group(2).strip()
    
    def _extract_base_statistics(self, content: str, parsed_data: Dict[str, Any]):
        """기본 통계 정보 추출 — 실제 10053 포맷에 맞춤"""
        # Table Stats — handle extra spaces in alias, optional (NOT ANALYZED), etc.
        # Table: BLOG_POST  Alias:  P  (NOT ANALYZED)
        #   #Rows: 82  SSZ: 0  LGR: 0  #Blks:  1  AvgRowLen:  100.00  NEB: 0  ...
        table_pattern = re.compile(
            r'Table:\s+(\S+)\s+Alias:\s+(\S+)([^\n]*)\n'
            r'\s+#Rows:\s+(\d+).*?#Blks:\s+(\d+)\s+AvgRowLen:\s+([\d.]+)',
            re.DOTALL
        )
        seen_tables = set()
        for m in table_pattern.finditer(content):
            table_name = m.group(1)
            if table_name in seen_tables:
                continue
            seen_tables.add(table_name)
            rest_of_line = m.group(3)
            not_analyzed = bool(re.search(r'NOT ANALYZED', rest_of_line, re.IGNORECASE))
            parsed_data['base_statistics'][table_name] = {
                'alias': m.group(2),
                'rows': int(m.group(4)),
                'blocks': int(m.group(5)),
                'avg_row_len': float(m.group(6)),
                'not_analyzed': not_analyzed,
            }

        # Index Stats — handle (NOT ANALYZED), KKEISFLG, SSZ, extra fields after NRW
        # Real formats:
        #   Index: IDX_BLOG_POST_AUTHOR  Col#: 5
        #   LVLS: 0  #LB: 0  #DK: 0  LB/K: 0.00  DB/K: 0.00  CLUF: 0.00  NRW: 0.00 SSZ: ...
        #   Index: PK_BLOG_POST  Col#: 1    (NOT ANALYZED)
        #   LVLS: 1  #LB: 25  ...
        #   Index: SYS_IL0000165827C00003$$  Col#:    (NOT ANALYZED)
        #   LVLS: 1  #LB: 25  ...
        index_pattern = re.compile(
            r'Index:\s+(\S+)\s+Col#:\s*([\d\s]*?)(?:\s*\(.*?\))?\s*\n'
            r'\s*LVLS:\s+(\d+)\s+#LB:\s+(\d+)\s+#DK:\s+(\d+)\s+LB/K:\s+([\d.]+)\s+DB/K:\s+([\d.]+)\s+CLUF:\s+([\d.]+)\s+NRW:\s+([\d.]+)',
        )
        indexes = {}
        for m in index_pattern.finditer(content):
            idx_name = m.group(1)
            if idx_name in indexes:
                continue
            # Check for NOT ANALYZED in the Col# line area
            full_match = m.group(0)
            not_analyzed = bool(re.search(r'NOT ANALYZED', full_match, re.IGNORECASE))
            indexes[idx_name] = {
                'columns': m.group(2).strip(),
                'levels': int(m.group(3)),
                'leaf_blocks': int(m.group(4)),
                'distinct_keys': int(m.group(5)),
                'lb_per_key': float(m.group(6)),
                'db_per_key': float(m.group(7)),
                'clustering_factor': float(m.group(8)),
                'num_rows': float(m.group(9)),
                'not_analyzed': not_analyzed,
            }
        parsed_data['index_statistics'] = indexes

        # Column 통계 추출 — Histogram line is optional, handle NO STATISTICS marker
        # Column (#11): STATUS(VARCHAR2)  NO STATISTICS (using defaults)
        #   AvgLen: 12 NDV: 3 Nulls: 0 Density: 0.390244
        col_pattern = re.compile(
            r'Column\s+\(#(\d+)\):\s+(\S+)\((\w+)\)([^\n]*)\n'
            r'\s+AvgLen:\s+(\d+)\s+NDV:\s+(\d+)\s+Nulls:\s+(\d+)\s+Density:\s+([\d.]+)'
            r'(?:\n\s+Histogram:\s+(\S+)\s+#Bkts:\s+(\d+)'
            r'(?:\s+UncompBkts:\s+(\d+))?'
            r'(?:\s+EndPtVals:\s+(\d+))?)?',
        )
        columns = {}
        for m in col_pattern.finditer(content):
            col_name = m.group(2)
            rest_of_line = m.group(4)
            no_stats = bool(re.search(r'NO STATISTICS', rest_of_line, re.IGNORECASE))
            columns[col_name] = {
                'col_num': int(m.group(1)),
                'data_type': m.group(3),
                'avg_len': int(m.group(5)),
                'ndv': int(m.group(6)),
                'nulls': int(m.group(7)),
                'density': float(m.group(8)),
                'histogram': m.group(9) if m.group(9) else ('None' if not no_stats else 'N/A (no stats)'),
                'buckets': int(m.group(10)) if m.group(10) else 0,
                'uncomp_buckets': int(m.group(11)) if m.group(11) else None,
                'end_pt_vals': int(m.group(12)) if m.group(12) else None,
                'no_statistics': no_stats,
            }
        parsed_data['column_statistics'] = columns

    def _extract_table_access_paths(self, content: str, parsed_data: Dict[str, Any]):
        """테이블 접근 경로 분석 — 실제 10053 포맷에 맞춤"""
        # "Access path analysis for TABLE_NAME" 블록 찾기
        access_blocks = re.split(r'Access path analysis for (\S+)', content)
        
        for i in range(1, len(access_blocks), 2):
            table_name = access_blocks[i]
            block_text = access_blocks[i+1] if i+1 < len(access_blocks) else ""
            # 다음 Access path analysis 또는 OPTIMIZER STATISTICS 전까지
            block_text = re.split(r'Access path analysis for|OPTIMIZER STATISTICS', block_text)[0]
            
            path_info = {
                'table_name': table_name,
                'cardinality': None,
                'access_methods': [],
                'best_access': None
            }
            
            # Cardinality 추출
            card_m = re.search(r'Card: Original:\s+([\d.]+)\s+Rounded:\s+(\d+)', block_text)
            if card_m:
                path_info['cardinality'] = int(card_m.group(2))
            
            # Access Path 추출 — 실제 포맷에 맞춤
            # Access Path: TableScan
            #   Cost:  18882.019786  Resp: 18882.019786  Degree: 0
            # Access Path: index (FFS)
            #   Cost:  3232.919726  Resp: 3232.919726  Degree: 1
            # Access Path: index (FullScan)
            #   Index: IDX_FS_CORP_YEAR
            #   ...
            #   Cost: 19300.081477  Resp: 19300.081477  Degree: 1
            
            lines = block_text.split('\n')
            current_method = None
            current_index = None
            current_resc_io = None
            current_resc_cpu = None
            current_ix_sel = None
            current_ix_sel_filters = None

            for j, line in enumerate(lines):
                # Access Path 라인 감지
                ap_m = re.match(r'\s*Access Path:\s+(.+)', line)
                if ap_m:
                    # New access path — if same method name as previous, merge info
                    new_method = ap_m.group(1).strip()
                    if new_method != current_method:
                        # Reset for new method
                        current_method = new_method
                        current_index = None
                        current_resc_io = None
                        current_resc_cpu = None
                        current_ix_sel = None
                        current_ix_sel_filters = None
                    continue

                # Index 이름 감지 (Access Path 바로 다음에 나올 수 있음)
                idx_m = re.match(r'\s*Index:\s+(\S+)', line)
                if idx_m and current_method:
                    current_index = idx_m.group(1)
                    continue

                # resc_io / resc_cpu 라인 감지
                resc_m = re.match(r'\s*resc_io:\s+([\d.]+)\s+resc_cpu:\s+(\d+)', line)
                if resc_m and current_method:
                    current_resc_io = float(resc_m.group(1))
                    current_resc_cpu = int(resc_m.group(2))
                    continue

                # ix_sel 라인 감지
                ixsel_m = re.match(r'\s*ix_sel:\s+([\d.]+)\s+ix_sel_with_filters:\s+([\d.]+)', line)
                if ixsel_m and current_method:
                    current_ix_sel = float(ixsel_m.group(1))
                    current_ix_sel_filters = float(ixsel_m.group(2))
                    continue

                # Cost 라인 감지
                cost_m = re.match(r'\s*Cost:\s+([\d.]+)\s+Resp:\s+([\d.]+)\s+Degree:\s+(\d+)', line)
                if cost_m and current_method:
                    entry = {
                        'method': current_method,
                        'index': current_index,
                        'cost': float(cost_m.group(1)),
                        'response_time': float(cost_m.group(2)),
                        'degree': int(cost_m.group(3)),
                        'cost_io': current_resc_io,
                        'cost_cpu': current_resc_cpu,
                        'ix_sel': current_ix_sel,
                        'ix_sel_with_filters': current_ix_sel_filters,
                    }
                    path_info['access_methods'].append(entry)
                    current_method = None
                    current_index = None
                    current_resc_io = None
                    current_resc_cpu = None
                    current_ix_sel = None
                    current_ix_sel_filters = None
                    continue

                # Cost_io / Cost_cpu breakdown (appears on the line after Cost)
                costio_m = re.match(r'\s*Cost_io:\s+([\d.]+)\s+Cost_cpu:\s+(\d+)', line)
                if costio_m and path_info['access_methods']:
                    # Attach to the last added method
                    last = path_info['access_methods'][-1]
                    last['cost_io'] = float(costio_m.group(1))
                    last['cost_cpu'] = int(costio_m.group(2))
                    continue
            
            # Best Access Path 추출
            # Best:: AccessPath: IndexFFS
            # Index: IDX_FS_CORP_YEAR
            #        Cost: 3232.919726  Degree: 1  Resp: 3232.919726  Card: 3802835.000000  Bytes: 0.000000
            best_m = re.search(
                r'Best::\s+AccessPath:\s+(\S+)\s*\n'
                r'(?:\s*Index:\s+(\S+)\s*\n)?'
                r'\s+Cost:\s+([\d.]+)\s+Degree:\s+(\d+)\s+Resp:\s+([\d.]+)\s+Card:\s+([\d.]+)',
                block_text
            )
            if best_m:
                path_info['best_access'] = {
                    'method': best_m.group(1),
                    'index': best_m.group(2),
                    'cost': float(best_m.group(3)),
                    'degree': int(best_m.group(4)),
                    'response_time': float(best_m.group(5)),
                    'cardinality': float(best_m.group(6)),
                }
            
            parsed_data['table_access_paths'].append(path_info)

    def _extract_join_orders(self, content: str, parsed_data: Dict[str, Any]):
        """조인 순서 분석 — 실제 10053 포맷에 맞춤"""
        # Join order[1]:  TB_FINANCIAL_STMT[TB_FINANCIAL_STMT]#0
        join_pattern = re.compile(
            r'Join order\[(\d+)\]:\s+(.+?)$',
            re.MULTILINE | re.IGNORECASE
        )
        for m in join_pattern.finditer(content):
            order_num = int(m.group(1))
            order_text = m.group(2).strip()
            # 테이블 추출: TABLE_NAME[ALIAS]#N
            tables = re.findall(r'(\w+)\[', order_text)
            parsed_data['join_orders'].append({
                'order_num': order_num,
                'tables': tables,
                'raw': order_text
            })
        
        # Best so far: Table#: 0  cost: 3435.821485  card: 3802835.000000  bytes: 22817010.000000
        best_pattern = re.compile(
            r'Best so far:\s+Table#:\s+(\d+)\s+cost:\s+([\d.]+)\s+card:\s+([\d.]+)\s+bytes:\s+([\d.]+)'
        )
        best_costs = []
        for m in best_pattern.finditer(content):
            best_costs.append({
                'table_num': int(m.group(1)),
                'cost': float(m.group(2)),
                'cardinality': float(m.group(3)),
                'bytes': float(m.group(4))
            })
        if best_costs:
            parsed_data['best_join_order'] = best_costs[-1]
        
        # Number of join permutations tried
        perm_m = re.search(r'Number of join permutations tried:\s+(\d+)', content)
        if perm_m:
            parsed_data['join_permutations_tried'] = int(perm_m.group(1))
        
        # Single table detection
        if parsed_data['join_permutations_tried'] == 1 and len(parsed_data.get('base_statistics', {})) <= 1:
            parsed_data['is_single_table'] = True

    def _analyze_costs(self, parsed_data: Dict[str, Any]):
        """비용 분석 — 접근 경로별 비용 비교"""
        cost_analysis = {}
        
        for path in parsed_data.get('table_access_paths', []):
            methods = path.get('access_methods', [])
            if methods:
                methods_sorted = sorted(methods, key=lambda x: x.get('cost', float('inf')))
                path['cheapest'] = methods_sorted[0]
                path['most_expensive'] = methods_sorted[-1]
                if len(methods_sorted) > 1 and methods_sorted[0]['cost'] > 0:
                    path['cost_ratio'] = methods_sorted[-1]['cost'] / methods_sorted[0]['cost']
                
                cost_analysis[path['table_name']] = {
                    'best_method': methods_sorted[0]['method'],
                    'best_index': methods_sorted[0].get('index'),
                    'best_cost': methods_sorted[0]['cost'],
                    'worst_cost': methods_sorted[-1]['cost'],
                    'alternatives': len(methods_sorted),
                }
        
        # Best overall
        best = parsed_data.get('best_join_order', {})
        if best:
            cost_analysis['_overall'] = {
                'total_cost': best.get('cost', 0),
                'total_card': best.get('cardinality', 0),
                'total_bytes': best.get('bytes', 0),
            }
        
        parsed_data['cost_analysis'] = cost_analysis

    def _extract_dynamic_sampling(self, content: str, parsed_data: Dict[str, Any]):
        """동적 샘플링 감지"""
        # Pattern: "Dynamic sampling updated table card"  or "** Dynamic sampling used for table"
        ds_pattern = re.compile(
            r'(?:Dynamic sampling|dynamic sampling)\s+.*?(?:table|index)\s+.*',
            re.IGNORECASE
        )
        entries = []
        for m in ds_pattern.finditer(content):
            line = m.group(0).strip()
            if line not in entries:
                entries.append(line)
        parsed_data['dynamic_sampling'] = entries

    def _extract_query_transformations(self, content: str, parsed_data: Dict[str, Any]):
        """쿼리 변환 추출 — PM, CBQT, TE, SU, JE, CVM, etc."""
        transformations = []

        # Predicate Move-Around
        pm_m = re.search(r'Predicate Move-Around \(PM\)', content)
        if pm_m:
            transformations.append({'type': 'PM', 'name': 'Predicate Move-Around', 'status': 'considered'})

        # CBQT
        cbqt_entries = re.findall(r'(CBQT[:\s]+.*)', content)
        for entry in cbqt_entries:
            status = 'applied' if 'Bypass' not in entry else 'bypassed'
            transformations.append({'type': 'CBQT', 'name': 'Cost-Based Query Transformation', 'status': status, 'detail': entry.strip()})

        # Table Expansion (TE)
        te_entries = re.findall(r'(TE:\s+.*)', content)
        for entry in te_entries:
            transformations.append({'type': 'TE', 'name': 'Table Expansion', 'status': 'considered', 'detail': entry.strip()})

        # Subquery Unnesting (SU)
        su_entries = re.findall(r'(SU:\s+.*)', content)
        for entry in su_entries:
            status = 'applied' if 'unnested' in entry.lower() else 'considered'
            transformations.append({'type': 'SU', 'name': 'Subquery Unnesting', 'status': status, 'detail': entry.strip()})

        # Join Elimination (JE)
        je_entries = re.findall(r'(JE:\s+.*)', content)
        for entry in je_entries:
            transformations.append({'type': 'JE', 'name': 'Join Elimination', 'status': 'considered', 'detail': entry.strip()})

        # Complex View Merging (CVM)
        cvm_entries = re.findall(r'(CVM:\s+.*)', content)
        for entry in cvm_entries:
            transformations.append({'type': 'CVM', 'name': 'Complex View Merging', 'status': 'considered', 'detail': entry.strip()})

        # Star Transformation (ST)
        st_entries = re.findall(r'(ST:\s+.*)', content)
        for entry in st_entries:
            transformations.append({'type': 'ST', 'name': 'Star Transformation', 'status': 'considered', 'detail': entry.strip()})

        # Or Expansion (ORE)
        ore_entries = re.findall(r'(ORE:\s+.*)', content)
        for entry in ore_entries:
            transformations.append({'type': 'ORE', 'name': 'Or Expansion', 'status': 'considered', 'detail': entry.strip()})

        # Generic: query transformation patterns
        generic_pattern = re.compile(r'query block .+ (#\d+)\s*transformed', re.IGNORECASE)
        for m in generic_pattern.finditer(content):
            transformations.append({'type': 'GENERIC', 'name': 'Query Block Transformation', 'status': 'applied', 'detail': m.group(0).strip()})

        parsed_data['query_transformations'] = transformations

    def _detect_issues(self, parsed_data: Dict[str, Any]):
        """이슈 감지 — 실무 관점. 반환: list of dict"""
        issues = []
        
        # 1. Full Table Scan이 최적인 경우 (대형 테이블)
        for path in parsed_data.get('table_access_paths', []):
            best = path.get('best_access', {})
            if best and best.get('method') == 'TableScan':
                rows = parsed_data.get('base_statistics', {}).get(path['table_name'], {}).get('rows', 0)
                if rows > 100000:
                    issues.append({
                        'severity': 'WARNING',
                        'type': 'Full Table Scan',
                        'message': f"{path['table_name']}: Full Table Scan이 최적 경로로 선택됨 ({rows:,}행)",
                        'recommendation': '인덱스 생성 또는 WHERE 조건 추가를 검토하세요. GROUP BY/ORDER BY 컬럼에 인덱스가 있는지 확인하세요.'
                    })
            
            # Index FFS가 선택된 경우 (인덱스로 커버 가능)
            if best and best.get('method') == 'IndexFFS':
                issues.append({
                    'severity': 'INFO',
                    'type': 'Index Fast Full Scan',
                    'message': f"{path['table_name']}: Index FFS({best.get('index', 'N/A')})가 최적 경로 — 테이블 접근 없이 인덱스만으로 처리",
                    'recommendation': '현재 최적. 쿼리가 인덱스 컬럼만 사용하고 있어 효율적입니다.'
                })
        
        # 2. 인덱스 클러스터링 팩터 높은 경우
        for idx_name, idx_info in parsed_data.get('index_statistics', {}).items():
            cf = idx_info.get('clustering_factor', 0)
            rows = idx_info.get('num_rows', 0)
            if rows > 0 and cf / rows > 0.3:
                severity = 'WARNING' if cf / rows > 0.5 else 'INFO'
                issues.append({
                    'severity': severity,
                    'type': 'High Clustering Factor',
                    'message': f"{idx_name}: Clustering Factor {cf:,.0f} / Rows {rows:,.0f} = {cf/rows:.1%}",
                    'recommendation': f'테이블 데이터가 인덱스 순서와 맞지 않음. ALTER TABLE MOVE + REBUILD INDEX 또는 CTAS 재구성을 고려하세요.'
                })
        
        # 3. 컬럼 통계 — NDV 적고 히스토그램 없는 경우
        for col_name, col_info in parsed_data.get('column_statistics', {}).items():
            if col_info.get('ndv', 0) <= 10 and col_info.get('histogram') == 'None':
                issues.append({
                    'severity': 'WARNING',
                    'type': 'Missing Histogram',
                    'message': f"{col_name}: NDV={col_info['ndv']}로 적은데 히스토그램 없음",
                    'recommendation': f'DBMS_STATS.GATHER_TABLE_STATS에서 METHOD_OPT => \'FOR COLUMNS {col_name} SIZE AUTO\' 로 히스토그램 수집을 권장합니다.'
                })
        
        # 4. 시스템 통계 — NOWORKLOAD 사용 시
        sys_stats = parsed_data.get('system_statistics', {})
        if sys_stats.get('stats_type') == 'NOWORKLOAD':
            issues.append({
                'severity': 'INFO',
                'type': 'NOWORKLOAD System Stats',
                'message': '시스템 통계가 NOWORKLOAD 모드 — 실제 I/O 특성이 반영되지 않음',
                'recommendation': 'DBMS_STATS.GATHER_SYSTEM_STATS(\'INTERVAL\', interval => 60)로 실제 워크로드 기반 수집을 권장합니다.'
            })
        
        # 5. MBRC가 NO VALUE인 경우
        mbrc = sys_stats.get('MBRC', {})
        if isinstance(mbrc, dict) and mbrc.get('is_default'):
            issues.append({
                'severity': 'WARNING',
                'type': 'MBRC Not Set',
                'message': f"MBRC 값 미설정 (기본값 {mbrc.get('default', '?')} 사용) — Full Scan 비용 계산에 영향",
                'recommendation': 'db_file_multiblock_read_count 또는 시스템 통계 수집으로 실제 MBRC를 반영하세요.'
            })

        # 6. NOT ANALYZED 테이블/인덱스 감지
        not_analyzed_tables = [t for t, info in parsed_data.get('base_statistics', {}).items() if info.get('not_analyzed')]
        not_analyzed_indexes = [i for i, info in parsed_data.get('index_statistics', {}).items() if info.get('not_analyzed')]
        if not_analyzed_tables:
            issues.append({
                'severity': 'WARNING',
                'type': 'Table NOT ANALYZED',
                'message': f"통계 미수집 테이블: {', '.join(not_analyzed_tables)}",
                'recommendation': 'DBMS_STATS.GATHER_TABLE_STATS로 테이블 통계를 수집하세요. 통계 미수집 시 옵티마이저가 부정확한 실행계획을 생성할 수 있습니다.'
            })
        if not_analyzed_indexes:
            issues.append({
                'severity': 'WARNING',
                'type': 'Index NOT ANALYZED',
                'message': f"통계 미수집 인덱스: {', '.join(not_analyzed_indexes)}",
                'recommendation': 'DBMS_STATS.GATHER_INDEX_STATS 또는 GATHER_TABLE_STATS(cascade=>TRUE)로 인덱스 통계를 수집하세요.'
            })

        # 7. 동적 샘플링 사용 감지
        ds_entries = parsed_data.get('dynamic_sampling', [])
        if ds_entries:
            issues.append({
                'severity': 'INFO',
                'type': 'Dynamic Sampling Used',
                'message': f"동적 샘플링 사용 ({len(ds_entries)}건): 옵티마이저가 런타임에 통계를 추정하고 있음",
                'recommendation': 'DBMS_STATS로 정확한 통계를 수집하면 동적 샘플링 없이도 안정적인 실행계획을 얻을 수 있습니다.'
            })

        parsed_data['issues'] = issues

    def generate_10053_report(self, parsed_data: Dict[str, Any], output_path: str) -> str:
        """HTML 리포트 생성"""
        self._log('info', f"10053 리포트 생성: {output_path}")
        
        try:
            html_content = self._generate_html_report(parsed_data)
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            self._log('info', f"10053 리포트 생성 완료: {output_path}")
            return output_path
            
        except Exception as e:
            self._log('error', f"리포트 생성 실패: {e}")
            return None
    
    def _generate_html_report(self, parsed_data: Dict[str, Any]) -> str:
        """HTML 리포트 내용 생성 — 전문적이고 포괄적인 리포트"""
        
        db_info = parsed_data.get('db_info', {})
        
        html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <title>10053 Optimizer Trace Analysis — {parsed_data.get('sql_id', 'N/A')}</title>
    <style>
        * {{ box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background: #f5f7fa; color: #333; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .header {{ background: linear-gradient(135deg, #1a237e 0%, #283593 100%); color: white; padding: 25px 30px; border-radius: 8px; margin-bottom: 20px; }}
        .header h1 {{ margin: 0 0 10px 0; font-size: 24px; }}
        .header .meta {{ display: flex; flex-wrap: wrap; gap: 20px; font-size: 13px; opacity: 0.9; }}
        .header .meta span {{ background: rgba(255,255,255,0.15); padding: 3px 10px; border-radius: 4px; }}
        .section {{ background: white; border-radius: 8px; padding: 20px 25px; margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        .section h2 {{ color: #1a237e; border-bottom: 2px solid #e8eaf6; padding-bottom: 8px; margin-top: 0; font-size: 18px; }}
        .section h3 {{ color: #333; margin-top: 20px; font-size: 15px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 10px 0; font-size: 13px; }}
        th, td {{ border: 1px solid #e0e0e0; padding: 8px 12px; text-align: left; }}
        th {{ background: #f5f5f5; font-weight: 600; color: #555; }}
        tr:nth-child(even) {{ background: #fafafa; }}
        .best-row {{ background: #e8f5e9 !important; font-weight: 600; }}
        .worst-row {{ background: #ffebee !important; }}
        .issue {{ border-left: 4px solid; padding: 12px 16px; margin: 8px 0; border-radius: 0 4px 4px 0; }}
        .issue-WARNING {{ background: #fff8e1; border-color: #ff8f00; }}
        .issue-INFO {{ background: #e8f5e9; border-color: #43a047; }}
        .issue-CRITICAL {{ background: #ffebee; border-color: #e53935; }}
        .issue .issue-title {{ font-weight: 600; margin-bottom: 4px; }}
        .issue .issue-rec {{ font-size: 12px; color: #666; margin-top: 6px; }}
        .sql-box {{ background: #263238; color: #e0e0e0; padding: 15px 20px; border-radius: 6px; font-family: 'Consolas', 'Courier New', monospace; font-size: 13px; white-space: pre-wrap; word-wrap: break-word; overflow-x: auto; }}
        .badge {{ display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 11px; font-weight: 600; }}
        .badge-best {{ background: #c8e6c9; color: #2e7d32; }}
        .badge-fts {{ background: #ffecb3; color: #e65100; }}
        .badge-idx {{ background: #bbdefb; color: #1565c0; }}
        .stat-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin: 10px 0; }}
        .stat-card {{ background: #f5f7fa; padding: 12px 16px; border-radius: 6px; text-align: center; }}
        .stat-card .value {{ font-size: 22px; font-weight: 700; color: #1a237e; }}
        .stat-card .label {{ font-size: 12px; color: #666; margin-top: 4px; }}
        details {{ margin: 5px 0; }}
        details summary {{ cursor: pointer; font-weight: 600; color: #1a237e; padding: 5px 0; }}
        details summary:hover {{ color: #3949ab; }}
        .cost-num {{ font-family: 'Consolas', monospace; }}
        .footer {{ text-align: center; color: #999; font-size: 12px; margin-top: 20px; padding: 10px; }}
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>🔍 10053 Optimizer Trace Analysis</h1>
        <div class="meta">
            <span>📋 SQL_ID: <strong>{parsed_data.get('sql_id', 'N/A')}</strong></span>
            <span>🖥️ {db_info.get('instance_name', 'N/A')} / {db_info.get('database_name', 'N/A')}</span>
            <span>📅 분석: {parsed_data['parse_time'].strftime('%Y-%m-%d %H:%M:%S')}</span>
            <span>📄 {os.path.basename(parsed_data['file_path'])}</span>
        </div>
    </div>
"""
        
        # ── 이슈 요약 ──
        issues = parsed_data.get('issues', [])
        if issues:
            warn_cnt = sum(1 for i in issues if i['severity'] == 'WARNING')
            info_cnt = sum(1 for i in issues if i['severity'] == 'INFO')
            html += f"""
    <div class="section">
        <h2>🚨 발견된 이슈 ({warn_cnt} 경고, {info_cnt} 정보)</h2>
"""
            for issue in issues:
                html += f"""
        <div class="issue issue-{issue['severity']}">
            <div class="issue-title">[{issue['severity']}] {issue['type']}</div>
            {issue['message']}
            <div class="issue-rec">💡 {issue['recommendation']}</div>
        </div>
"""
            html += "    </div>\n"
        
        # ── SQL 텍스트 ──
        sql_text = parsed_data.get('sql_text', '')
        if sql_text:
            html += f"""
    <div class="section">
        <h2>📝 SQL Statement</h2>
        <div class="sql-box">{sql_text}</div>
    </div>
"""
        
        # ── 시스템 통계 ──
        sys_stats = parsed_data.get('system_statistics', {})
        if sys_stats:
            stats_type = sys_stats.get('stats_type', 'Unknown')
            html += f"""
    <div class="section">
        <h2>⚡ 시스템 통계 ({stats_type} Stats)</h2>
        <div class="stat-grid">
"""
            for name, info in sys_stats.items():
                if name == 'stats_type':
                    continue
                if isinstance(info, dict):
                    val = info.get('value', 'N/A')
                    unit = info.get('unit', '')
                    default = info.get('default', '')
                    is_def = '(기본값)' if info.get('is_default') else ''
                    html += f"""
            <div class="stat-card">
                <div class="value">{val}</div>
                <div class="label">{name} {unit} {is_def}<br><small>default: {default}</small></div>
            </div>
"""
            html += """
        </div>
    </div>
"""
        
        # ── 테이블 통계 ──
        base_stats = parsed_data.get('base_statistics', {})
        if base_stats:
            html += """
    <div class="section">
        <h2>📊 테이블 통계</h2>
        <table>
            <tr><th>테이블</th><th>별칭</th><th>행 수</th><th>블록 수</th><th>평균 행 길이</th><th>예상 크기(MB)</th></tr>
"""
            for table, stats in base_stats.items():
                size_mb = (stats['blocks'] * 8192) / (1024 * 1024)  # 8KB block size
                html += f"""
            <tr>
                <td><strong>{table}</strong></td>
                <td>{stats['alias']}</td>
                <td class="cost-num">{stats['rows']:,}</td>
                <td class="cost-num">{stats['blocks']:,}</td>
                <td>{stats['avg_row_len']}</td>
                <td class="cost-num">{size_mb:,.1f} MB</td>
            </tr>
"""
            html += "        </table>\n    </div>\n"
        
        # ── 인덱스 통계 ──
        idx_stats = parsed_data.get('index_statistics', {})
        if idx_stats:
            html += """
    <div class="section">
        <h2>📑 인덱스 통계</h2>
        <table>
            <tr><th>인덱스</th><th>컬럼#</th><th>Levels</th><th>Leaf Blocks</th><th>Distinct Keys</th><th>LB/K</th><th>DB/K</th><th>Clustering Factor</th><th>CF/Rows</th></tr>
"""
            for idx_name, info in idx_stats.items():
                cf_ratio = info['clustering_factor'] / info['num_rows'] if info['num_rows'] > 0 else 0
                cf_class = ' class="worst-row"' if cf_ratio > 0.5 else (' class="best-row"' if cf_ratio < 0.1 else '')
                html += f"""
            <tr{cf_class}>
                <td><strong>{idx_name}</strong></td>
                <td>{info['columns']}</td>
                <td>{info['levels']}</td>
                <td class="cost-num">{info['leaf_blocks']:,}</td>
                <td class="cost-num">{info['distinct_keys']:,}</td>
                <td>{info['lb_per_key']:.2f}</td>
                <td>{info['db_per_key']:.2f}</td>
                <td class="cost-num">{info['clustering_factor']:,.0f}</td>
                <td><strong>{cf_ratio:.1%}</strong></td>
            </tr>
"""
            html += "        </table>\n    </div>\n"
        
        # ── 컬럼 통계 ──
        col_stats = parsed_data.get('column_statistics', {})
        if col_stats:
            html += """
    <div class="section">
        <h2>📉 컬럼 통계</h2>
        <table>
            <tr><th>컬럼</th><th>타입</th><th>NDV</th><th>Nulls</th><th>Density</th><th>AvgLen</th><th>Histogram</th><th>Buckets</th></tr>
"""
            for col_name, info in col_stats.items():
                hist_badge = f'<span class="badge badge-idx">{info["histogram"]}</span>' if info['histogram'] != 'None' else '<span class="badge badge-fts">None</span>'
                html += f"""
            <tr>
                <td><strong>{col_name}</strong></td>
                <td>{info['data_type']}</td>
                <td class="cost-num">{info['ndv']:,}</td>
                <td>{info['nulls']:,}</td>
                <td>{info['density']:.6f}</td>
                <td>{info['avg_len']}</td>
                <td>{hist_badge}</td>
                <td>{info['buckets']:,}</td>
            </tr>
"""
            html += "        </table>\n    </div>\n"
        
        # ── 테이블 접근 경로 ──
        if parsed_data['table_access_paths']:
            html += """
    <div class="section">
        <h2>🔍 테이블 접근 경로 분석</h2>
"""
            for path_info in parsed_data['table_access_paths']:
                card = path_info.get('cardinality')
                card_str = f" (Cardinality: {card:,})" if card else ""
                html += f"        <h3>📋 {path_info['table_name']}{card_str}</h3>\n"
                
                if path_info['access_methods']:
                    sorted_methods = sorted(path_info['access_methods'], key=lambda x: x.get('cost', float('inf')))
                    best_cost = sorted_methods[0]['cost'] if sorted_methods else 0
                    
                    html += """
        <table>
            <tr><th>#</th><th>접근 방법</th><th>인덱스</th><th>비용</th><th>Cost_io</th><th>Cost_cpu</th><th>ix_sel</th><th>응답 시간</th><th>병렬도</th><th>비용 비율</th></tr>
"""
                    for rank, method in enumerate(sorted_methods, 1):
                        row_class = 'best-row' if rank == 1 else ('worst-row' if rank == len(sorted_methods) and len(sorted_methods) > 1 else '')
                        idx_name = method.get('index', '-')
                        ratio = method['cost'] / best_cost if best_cost > 0 else 0
                        ratio_str = '✅ 최적' if rank == 1 else f'{ratio:.1f}x'
                        badge = ''
                        if 'TableScan' in method['method']:
                            badge = ' <span class="badge badge-fts">FTS</span>'
                        elif 'FFS' in method['method']:
                            badge = ' <span class="badge badge-idx">FFS</span>'
                        elif 'FullScan' in method['method']:
                            badge = ' <span class="badge badge-fts">Full</span>'
                        
                        cost_io = method.get('cost_io')
                        cost_cpu = method.get('cost_cpu')
                        ix_sel = method.get('ix_sel')
                        cost_io_str = f'{cost_io:,.6f}' if cost_io is not None else '-'
                        cost_cpu_str = f'{cost_cpu:,}' if cost_cpu is not None else '-'
                        ix_sel_str = f'{ix_sel:.6f}' if ix_sel is not None else '-'
                        
                        html += f"""
            <tr class="{row_class}">
                <td>{rank}</td>
                <td>{method['method']}{badge}</td>
                <td>{idx_name}</td>
                <td class="cost-num">{method['cost']:,.2f}</td>
                <td class="cost-num">{cost_io_str}</td>
                <td class="cost-num">{cost_cpu_str}</td>
                <td class="cost-num">{ix_sel_str}</td>
                <td class="cost-num">{method['response_time']:,.2f}</td>
                <td>{method['degree']}</td>
                <td>{ratio_str}</td>
            </tr>
"""
                    html += "        </table>\n"
                
                # Best access 요약
                best = path_info.get('best_access')
                if best:
                    html += f"""
        <p>🏆 <strong>최적 경로:</strong> {best['method']}"""
                    if best.get('index'):
                        html += f" (Index: {best['index']})"
                    html += f" — Cost: <strong class=\"cost-num\">{best['cost']:,.2f}</strong>, Card: {best['cardinality']:,.0f}</p>\n"
            
            html += "    </div>\n"
        
        # ── 조인 순서 / 비용 요약 ──
        html += """
    <div class="section">
        <h2>🔄 실행 계획 비용 요약</h2>
"""
        if parsed_data.get('is_single_table'):
            best = parsed_data.get('best_join_order', {})
            html += f"""
        <p><span class="badge badge-idx">Single Table Query</span> — 조인 없음 (테이블 1개)</p>
        <div class="stat-grid">
            <div class="stat-card">
                <div class="value">{best.get('cost', 0):,.2f}</div>
                <div class="label">총 비용 (Cost)</div>
            </div>
            <div class="stat-card">
                <div class="value">{best.get('cardinality', 0):,.0f}</div>
                <div class="label">카디널리티</div>
            </div>
            <div class="stat-card">
                <div class="value">{best.get('bytes', 0):,.0f}</div>
                <div class="label">바이트</div>
            </div>
            <div class="stat-card">
                <div class="value">{parsed_data.get('join_permutations_tried', 0)}</div>
                <div class="label">순열 시도 수</div>
            </div>
        </div>
"""
        elif parsed_data.get('join_orders'):
            html += """
        <table>
            <tr><th>순서</th><th>조인 순서</th></tr>
"""
            for order in parsed_data['join_orders']:
                tables_str = ' → '.join(order['tables']) if order['tables'] else order['raw']
                html += f"""
            <tr>
                <td>{order['order_num']}</td>
                <td>{tables_str}</td>
            </tr>
"""
            html += "        </table>\n"
            
            best = parsed_data.get('best_join_order', {})
            if best:
                html += f"""
        <p>🏆 <strong>최종 비용:</strong> <span class="cost-num">{best.get('cost', 0):,.2f}</span> | 
        Card: {best.get('cardinality', 0):,.0f} | 
        Bytes: {best.get('bytes', 0):,.0f} | 
        순열 시도: {parsed_data.get('join_permutations_tried', 0)}</p>
"""
        
        html += "    </div>\n"
        
        # ── 옵티마이저 파라미터 (Altered) ──
        altered = parsed_data.get('optimizer_parameters_altered', {})
        default = parsed_data.get('optimizer_parameters_default', {})
        
        if altered:
            html += """
    <div class="section">
        <h2>⚙️ 옵티마이저 파라미터</h2>
        <h3>📌 변경된 파라미터 (Altered Values)</h3>
        <table>
            <tr><th>파라미터</th><th>값</th></tr>
"""
            for param, value in altered.items():
                html += f"            <tr><td><strong>{param}</strong></td><td class=\"cost-num\">{value}</td></tr>\n"
            html += "        </table>\n"
        
        if default:
            # 주요 파라미터만 기본 표시, 나머지는 접기
            key_params = {
                'optimizer_mode', 'optimizer_features_enable', 'cpu_count', 'active_instance_count',
                'db_file_multiblock_read_count', 'pga_aggregate_target', 'hash_area_size',
                'sort_area_size', 'cursor_sharing', 'star_transformation_enabled',
                'parallel_threads_per_cpu', '_optimizer_cost_model', '_b_tree_bitmap_plans',
                '_optimizer_skip_scan_enabled', '_optimizer_batch_table_access_by_rowid',
            }
            key_defaults = {k: v for k, v in default.items() if k in key_params}
            other_defaults = {k: v for k, v in default.items() if k not in key_params}
            
            if key_defaults:
                html += """
        <h3>📋 주요 기본 파라미터</h3>
        <table>
            <tr><th>파라미터</th><th>값</th></tr>
"""
                for param, value in key_defaults.items():
                    html += f"            <tr><td>{param}</td><td class=\"cost-num\">{value}</td></tr>\n"
                html += "        </table>\n"
            
            if other_defaults:
                html += f"""
        <details>
            <summary>📂 기타 기본 파라미터 ({len(other_defaults)}개) — 클릭하여 펼치기</summary>
            <table>
                <tr><th>파라미터</th><th>값</th></tr>
"""
                for param, value in other_defaults.items():
                    html += f"                <tr><td>{param}</td><td class=\"cost-num\">{value}</td></tr>\n"
                html += "            </table>\n        </details>\n"
        
        html += "    </div>\n" if (altered or default) else ""
        
        # ── 쿼리 변환 ──
        qt_list = parsed_data.get('query_transformations', [])
        if qt_list:
            # 중복 제거 및 유형별 그룹핑
            qt_types = {}
            for qt in qt_list:
                t = qt.get('type', 'UNKNOWN')
                if t not in qt_types:
                    qt_types[t] = {'name': qt.get('name', t), 'entries': [], 'statuses': set()}
                qt_types[t]['entries'].append(qt)
                qt_types[t]['statuses'].add(qt.get('status', ''))
            
            html += f"""
    <div class="section">
        <h2>🔄 쿼리 변환 (Query Transformations) — {len(qt_types)}개 유형, {len(qt_list)}건</h2>
        <table>
            <tr><th>유형</th><th>변환명</th><th>건수</th><th>상태</th></tr>
"""
            for t, info in qt_types.items():
                statuses = ', '.join(sorted(info['statuses']))
                status_badge = ''
                if 'applied' in info['statuses']:
                    status_badge = '<span class="badge badge-best">applied</span> '
                if 'bypassed' in info['statuses']:
                    status_badge += '<span class="badge badge-fts">bypassed</span> '
                if 'considered' in info['statuses']:
                    status_badge += '<span class="badge badge-idx">considered</span>'
                html += f"""
            <tr>
                <td><strong>{t}</strong></td>
                <td>{info['name']}</td>
                <td class="cost-num">{len(info['entries'])}</td>
                <td>{status_badge}</td>
            </tr>
"""
            html += "        </table>\n"
            
            # 상세 내용 접기
            html += f"""
        <details>
            <summary>📂 변환 상세 ({len(qt_list)}건) — 클릭하여 펼치기</summary>
            <table>
                <tr><th>유형</th><th>상태</th><th>상세</th></tr>
"""
            for qt in qt_list:
                detail = qt.get('detail', '')
                if detail:
                    html += f"""
                <tr>
                    <td>{qt.get('type', '')}</td>
                    <td>{qt.get('status', '')}</td>
                    <td style="font-size:12px">{detail[:200]}</td>
                </tr>
"""
            html += "            </table>\n        </details>\n    </div>\n"
        
        # ── Footer ──
        html += f"""
    <div class="footer">
        Generated by Oracle SQL Tuning Pipeline — 10053 Trace Analyzer v2.0<br>
        {db_info.get('oracle_version', '')} | {db_info.get('node_name', '')} | {parsed_data['parse_time'].strftime('%Y-%m-%d %H:%M:%S')}
    </div>
</div>
</body>
</html>"""
        
        return html


# ============================================================
# 외부 호출용 함수
# ============================================================

def collect_10053_trace(sql_id: str, config: Dict[str, Any], logger, sql_text: str = None) -> Optional[str]:
    """10053 트레이스 수집 함수 (외부 호출용)"""
    collector = OptimizerTraceCollector(config, logger)
    return collector.collect_10053(sql_id, sql_text)


def analyze_10053_trace(trace_file_path: str, config: Dict[str, Any], logger) -> Dict[str, Any]:
    """10053 트레이스 분석 함수 (외부 호출용)"""
    analyzer = OptimizerTraceAnalyzer(config, logger)
    return analyzer.parse_10053(trace_file_path)


def generate_10053_report(parsed_data: Dict[str, Any], output_path: str, config: Dict[str, Any], logger) -> str:
    """10053 리포트 생성 함수 (외부 호출용)"""
    analyzer = OptimizerTraceAnalyzer(config, logger)
    return analyzer.generate_10053_report(parsed_data, output_path)


if __name__ == "__main__":
    # 테스트용 코드
    import logging
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    config = load_config()
    
    # 예시: 트레이스 수집
    sql_id = "abc123def456"
    sql_text = "SELECT * FROM employees e JOIN departments d ON e.department_id = d.department_id WHERE e.salary > 50000"
    
    trace_file = collect_10053_trace(sql_id, config, logger, sql_text)
    
    if trace_file:
        # 분석
        parsed_data = analyze_10053_trace(trace_file, config, logger)
        
        # 리포트 생성
        report_path = f"output/reports/10053_{sql_id}_report.html"
        generate_10053_report(parsed_data, report_path, config, logger)
