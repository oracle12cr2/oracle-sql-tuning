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
            
            # 1. 10053 트레이스 활성화
            self.logger.info("10053 트레이스 활성화")
            cursor.execute("ALTER SESSION SET EVENTS '10053 trace name context forever, level 1'")
            
            # 2. SQL 실행 (EXPLAIN PLAN 또는 실제 실행)
            collection_method = self.optimizer_config.get('collection_method', 'explain')
            
            if collection_method == 'explain':
                # EXPLAIN PLAN 방식
                if sql_text:
                    self.logger.info("EXPLAIN PLAN 실행")
                    cursor.execute(f"EXPLAIN PLAN FOR {sql_text}")
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
                        cursor.execute(f"EXPLAIN PLAN FOR {sql_text}")
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
            self.ssh_client.execute_command(f"touch /tmp/trace_start_time")
            
            # 트레이스 파일 찾기
            trace_files = self.ssh_client.execute_command(find_cmd).strip().split('\n')
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
    
    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger
        self.optimizer_config = config.get('optimizer_trace', {})
    
    def parse_10053(self, trace_file_path: str) -> Dict[str, Any]:
        """
        10053 트레이스 파일 파싱
        
        Args:
            trace_file_path: 트레이스 파일 경로
            
        Returns:
            파싱된 데이터 딕셔너리
        """
        self.logger.info(f"10053 트레이스 분석 시작: {trace_file_path}")
        
        parsed_data = {
            'file_path': trace_file_path,
            'parse_time': datetime.now(),
            'optimizer_parameters': {},
            'base_statistics': {},
            'table_access_paths': [],
            'join_orders': [],
            'best_join_order': {},
            'cost_analysis': {},
            'issues': []
        }
        
        try:
            with open(trace_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # 1. 옵티마이저 파라미터 추출
            self._extract_optimizer_parameters(content, parsed_data)
            
            # 2. 기본 통계 정보 추출
            self._extract_base_statistics(content, parsed_data)
            
            # 3. 테이블 접근 경로 분석
            self._extract_table_access_paths(content, parsed_data)
            
            # 4. 조인 순서 분석
            self._extract_join_orders(content, parsed_data)
            
            # 5. 비용 분석
            self._analyze_costs(parsed_data)
            
            # 6. 이슈 감지
            self._detect_issues(parsed_data)
            
            self.logger.info("10053 트레이스 분석 완료")
            return parsed_data
            
        except Exception as e:
            self.logger.error(f"10053 트레이스 분석 실패: {e}")
            return parsed_data
    
    def _extract_optimizer_parameters(self, content: str, parsed_data: Dict[str, Any]):
        """옵티마이저 파라미터 추출"""
        param_pattern = r'(\w+)\s*=\s*(\S+)'
        
        # Parameters 섹션 찾기
        params_section = re.search(r'Parameters used by the optimizer:(.*?)(?=\n\n|\nTable)', content, re.DOTALL)
        if params_section:
            params_text = params_section.group(1)
            for match in re.finditer(param_pattern, params_text):
                param_name, param_value = match.groups()
                parsed_data['optimizer_parameters'][param_name] = param_value
    
    def _extract_base_statistics(self, content: str, parsed_data: Dict[str, Any]):
        """기본 통계 정보 추출"""
        # BASE STATISTICAL INFORMATION 섹션 찾기
        stats_pattern = r'BASE STATISTICAL INFORMATION(.*?)(?=\n\n|\n\*\*\*|\nTABLE)'
        stats_match = re.search(stats_pattern, content, re.DOTALL)
        
        if stats_match:
            stats_text = stats_match.group(1)
            
            # 테이블 통계 추출
            table_stats_pattern = r'Table Stats::\s*Table:\s*(\w+)\s+Alias:\s*(\w+)\s*#Rows:\s*(\d+)\s+#Blks:\s*(\d+)\s+AvgRowLen:\s*(\d+)'
            
            for match in re.finditer(table_stats_pattern, stats_text):
                table_name = match.group(1)
                alias = match.group(2)
                rows = int(match.group(3))
                blocks = int(match.group(4))
                avg_row_len = int(match.group(5))
                
                parsed_data['base_statistics'][table_name] = {
                    'alias': alias,
                    'rows': rows,
                    'blocks': blocks,
                    'avg_row_len': avg_row_len
                }
    
    def _extract_table_access_paths(self, content: str, parsed_data: Dict[str, Any]):
        """테이블 접근 경로 분석"""
        # SINGLE TABLE ACCESS PATH 섹션들 찾기
        access_path_pattern = r'SINGLE TABLE ACCESS PATH(.*?)(?=\n\n|\nSINGLE TABLE ACCESS PATH|\nJOIN ORDER)'
        
        for match in re.finditer(access_path_pattern, content, re.DOTALL):
            path_text = match.group(1)
            
            # 테이블명 추출
            table_match = re.search(r'Table:\s*(\w+)', path_text)
            if not table_match:
                continue
                
            table_name = table_match.group(1)
            
            path_info = {
                'table_name': table_name,
                'access_methods': []
            }
            
            # 각 접근 방법의 비용 추출
            cost_pattern = r'(\w+(?:\s+\w+)*)\s+Cost:\s*(\d+)\s+Resp:\s*(\d+)\s+Degree:\s*(\d+)'
            
            for cost_match in re.finditer(cost_pattern, path_text):
                access_method = cost_match.group(1).strip()
                cost = int(cost_match.group(2))
                response_time = int(cost_match.group(3))
                degree = int(cost_match.group(4))
                
                path_info['access_methods'].append({
                    'method': access_method,
                    'cost': cost,
                    'response_time': response_time,
                    'degree': degree
                })
            
            parsed_data['table_access_paths'].append(path_info)
    
    def _extract_join_orders(self, content: str, parsed_data: Dict[str, Any]):
        """조인 순서 분석"""
        # JOIN ORDER 섹션 찾기
        join_order_pattern = r'JOIN ORDER\[(\d+)\]:(.*?)(?=\nJOIN ORDER|\nBEST JOIN ORDER|\n\*\*\*)'
        
        for match in re.finditer(join_order_pattern, content, re.DOTALL):
            order_num = int(match.group(1))
            order_text = match.group(2)
            
            # 조인 순서와 비용 추출
            cost_match = re.search(r'Cost:\s*(\d+)', order_text)
            cost = int(cost_match.group(1)) if cost_match else 0
            
            # 테이블 순서 추출
            table_order = []
            table_pattern = r'(\w+)\s*\[\d+\]'
            for table_match in re.finditer(table_pattern, order_text):
                table_order.append(table_match.group(1))
            
            parsed_data['join_orders'].append({
                'order_num': order_num,
                'tables': table_order,
                'cost': cost,
                'details': order_text.strip()
            })
        
        # BEST JOIN ORDER 추출
        best_join_pattern = r'BEST JOIN ORDER:(.*?)(?=\n\n|\n\*\*\*)'
        best_match = re.search(best_join_pattern, content, re.DOTALL)
        
        if best_match:
            best_text = best_match.group(1)
            cost_match = re.search(r'Cost:\s*(\d+)', best_text)
            cost = int(cost_match.group(1)) if cost_match else 0
            
            parsed_data['best_join_order'] = {
                'cost': cost,
                'details': best_text.strip()
            }
    
    def _analyze_costs(self, parsed_data: Dict[str, Any]):
        """비용 분석"""
        join_orders = parsed_data['join_orders']
        
        if len(join_orders) >= 2:
            # 비용 순으로 정렬
            sorted_orders = sorted(join_orders, key=lambda x: x['cost'])
            
            best_cost = sorted_orders[0]['cost']
            second_best_cost = sorted_orders[1]['cost'] if len(sorted_orders) > 1 else best_cost
            
            cost_diff_threshold = self.optimizer_config.get('analysis_rules', {}).get('cost_diff_threshold', 20)
            
            parsed_data['cost_analysis'] = {
                'best_cost': best_cost,
                'second_best_cost': second_best_cost,
                'cost_difference_pct': ((second_best_cost - best_cost) / best_cost * 100) if best_cost > 0 else 0,
                'significant_difference': ((second_best_cost - best_cost) / best_cost * 100) > cost_diff_threshold if best_cost > 0 else False
            }
    
    def _detect_issues(self, parsed_data: Dict[str, Any]):
        """이슈 자동 감지"""
        issues = []
        analysis_rules = self.optimizer_config.get('analysis_rules', {})
        
        # 1. 통계 정보 부정확성 체크
        stale_stats_days = analysis_rules.get('stale_stats_days', 30)
        
        # 2. 카디널리티 추정 오차 체크 (실제 구현 시 V$SQL_PLAN_STATISTICS와 비교)
        cardinality_threshold = analysis_rules.get('cardinality_error_threshold', 10)
        
        # 3. 비용 차이 분석
        cost_analysis = parsed_data.get('cost_analysis', {})
        if cost_analysis.get('significant_difference', False):
            issues.append({
                'type': 'COST_DIFFERENCE',
                'severity': 'WARNING',
                'message': f"조인 순서별 비용 차이가 큼 ({cost_analysis.get('cost_difference_pct', 0):.1f}%)",
                'recommendation': "통계 정보 갱신 또는 힌트 사용 검토"
            })
        
        # 4. 테이블 접근 방법 분석
        for table_access in parsed_data['table_access_paths']:
            access_methods = table_access['access_methods']
            if access_methods:
                # Full Table Scan이 가장 효율적인 경우 체크
                best_method = min(access_methods, key=lambda x: x['cost'])
                if 'FULL' in best_method['method'].upper():
                    issues.append({
                        'type': 'FULL_TABLE_SCAN',
                        'severity': 'INFO',
                        'message': f"테이블 {table_access['table_name']}에 Full Table Scan이 최적으로 선택됨",
                        'recommendation': "데이터량 대비 인덱스 효율성 검토"
                    })
        
        parsed_data['issues'] = issues
    
    def generate_10053_report(self, parsed_data: Dict[str, Any], output_path: str) -> str:
        """HTML 리포트 생성"""
        self.logger.info(f"10053 리포트 생성: {output_path}")
        
        try:
            html_content = self._generate_html_report(parsed_data)
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            self.logger.info(f"10053 리포트 생성 완료: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"리포트 생성 실패: {e}")
            return None
    
    def _generate_html_report(self, parsed_data: Dict[str, Any]) -> str:
        """HTML 리포트 내용 생성"""
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>10053 Optimizer Trace Analysis</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
                .section {{ margin-bottom: 30px; }}
                .section h2 {{ color: #333; border-bottom: 2px solid #ccc; padding-bottom: 5px; }}
                table {{ border-collapse: collapse; width: 100%; margin-bottom: 15px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .issue-warning {{ background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 10px; margin: 5px 0; }}
                .issue-info {{ background-color: #d4edda; border-left: 4px solid #28a745; padding: 10px; margin: 5px 0; }}
                .cost-highlight {{ background-color: #e3f2fd; }}
                pre {{ background-color: #f8f9fa; padding: 10px; border-radius: 3px; overflow-x: auto; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>10053 Optimizer Trace Analysis Report</h1>
                <p><strong>파일:</strong> {parsed_data['file_path']}</p>
                <p><strong>분석 시간:</strong> {parsed_data['parse_time'].strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        """
        
        # 이슈 요약
        if parsed_data['issues']:
            html += """
            <div class="section">
                <h2>🚨 발견된 이슈</h2>
            """
            for issue in parsed_data['issues']:
                css_class = 'issue-warning' if issue['severity'] == 'WARNING' else 'issue-info'
                html += f"""
                <div class="{css_class}">
                    <strong>{issue['type']}:</strong> {issue['message']}<br>
                    <em>권장사항:</em> {issue['recommendation']}
                </div>
                """
            html += "</div>"
        
        # 옵티마이저 파라미터
        if parsed_data['optimizer_parameters']:
            html += """
            <div class="section">
                <h2>⚙️ 옵티마이저 파라미터</h2>
                <table>
                    <tr><th>파라미터</th><th>값</th></tr>
            """
            for param, value in parsed_data['optimizer_parameters'].items():
                html += f"<tr><td>{param}</td><td>{value}</td></tr>"
            html += "</table></div>"
        
        # 기본 통계 정보
        if parsed_data['base_statistics']:
            html += """
            <div class="section">
                <h2>📊 테이블 통계 정보</h2>
                <table>
                    <tr><th>테이블</th><th>별칭</th><th>행 수</th><th>블록 수</th><th>평균 행 길이</th></tr>
            """
            for table, stats in parsed_data['base_statistics'].items():
                html += f"""
                <tr>
                    <td>{table}</td>
                    <td>{stats['alias']}</td>
                    <td>{stats['rows']:,}</td>
                    <td>{stats['blocks']:,}</td>
                    <td>{stats['avg_row_len']}</td>
                </tr>
                """
            html += "</table></div>"
        
        # 테이블 접근 경로
        if parsed_data['table_access_paths']:
            html += """
            <div class="section">
                <h2>🔍 테이블 접근 경로</h2>
            """
            for table_access in parsed_data['table_access_paths']:
                html += f"<h3>테이블: {table_access['table_name']}</h3>"
                if table_access['access_methods']:
                    html += """
                    <table>
                        <tr><th>접근 방법</th><th>비용</th><th>응답 시간</th><th>병렬도</th></tr>
                    """
                    # 비용 순으로 정렬하여 최적 방법 강조
                    sorted_methods = sorted(table_access['access_methods'], key=lambda x: x['cost'])
                    for i, method in enumerate(sorted_methods):
                        css_class = 'cost-highlight' if i == 0 else ''
                        html += f"""
                        <tr class="{css_class}">
                            <td>{method['method']}</td>
                            <td>{method['cost']}</td>
                            <td>{method['response_time']}</td>
                            <td>{method['degree']}</td>
                        </tr>
                        """
                    html += "</table>"
            html += "</div>"
        
        # 조인 순서 분석
        if parsed_data['join_orders']:
            html += """
            <div class="section">
                <h2>🔄 조인 순서 분석</h2>
                <table>
                    <tr><th>순서</th><th>테이블 순서</th><th>비용</th></tr>
            """
            # 비용 순으로 정렬
            sorted_orders = sorted(parsed_data['join_orders'], key=lambda x: x['cost'])
            for i, order in enumerate(sorted_orders):
                css_class = 'cost-highlight' if i == 0 else ''
                tables_str = ' → '.join(order['tables']) if order['tables'] else 'N/A'
                html += f"""
                <tr class="{css_class}">
                    <td>{order['order_num']}</td>
                    <td>{tables_str}</td>
                    <td>{order['cost']:,}</td>
                </tr>
                """
            html += "</table>"
            
            # 최종 선택된 조인 순서
            if parsed_data['best_join_order']:
                html += f"""
                <h3>✅ 최종 선택된 조인 순서</h3>
                <p><strong>비용:</strong> {parsed_data['best_join_order']['cost']:,}</p>
                <pre>{parsed_data['best_join_order']['details']}</pre>
                """
            
            html += "</div>"
        
        # 비용 분석
        cost_analysis = parsed_data.get('cost_analysis', {})
        if cost_analysis:
            html += f"""
            <div class="section">
                <h2>💰 비용 분석</h2>
                <table>
                    <tr><th>항목</th><th>값</th></tr>
                    <tr><td>최적 비용</td><td>{cost_analysis.get('best_cost', 0):,}</td></tr>
                    <tr><td>차선책 비용</td><td>{cost_analysis.get('second_best_cost', 0):,}</td></tr>
                    <tr><td>비용 차이</td><td>{cost_analysis.get('cost_difference_pct', 0):.1f}%</td></tr>
                </table>
            </div>
            """
        
        html += """
        </body>
        </html>
        """
        
        return html


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