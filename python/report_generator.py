#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
통합 보고서 생성 모듈 - 10053 옵티마이저 분석 포함
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Any
from jinja2 import Template


class ReportGenerator:
    """통합 보고서 생성기"""
    
    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger
        self.output_dir = os.path.join(
            config['output']['base_directory'],
            config['output']['directories']['reports']
        )
        
    def generate_comprehensive_report(self, all_results: Dict[str, Any]) -> str:
        """종합 보고서 생성 (10053 분석 포함)"""
        self.logger.info("종합 보고서 생성 시작")
        
        try:
            # 보고서 데이터 준비
            report_data = self._prepare_report_data(all_results)
            
            # HTML 생성
            html_content = self._generate_html_report(report_data)
            
            # 파일 저장
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_filename = f"comprehensive_report_{timestamp}.html"
            report_path = os.path.join(self.output_dir, report_filename)
            
            os.makedirs(self.output_dir, exist_ok=True)
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            self.logger.info(f"종합 보고서 생성 완료: {report_path}")
            return report_path
            
        except Exception as e:
            self.logger.error(f"종합 보고서 생성 실패: {e}")
            return None
    
    def _prepare_report_data(self, all_results: Dict[str, Any]) -> Dict[str, Any]:
        """보고서 데이터 준비"""
        report_data = {
            'generation_time': datetime.now(),
            'summary': {},
            'slow_sql_analysis': {},
            'trace_analysis': {},
            'optimizer_analysis': {},  # 10053 분석 결과
            'recommendations': [],
            'statistics': {}
        }
        
        # 1. 요약 정보
        slow_sqls = all_results.get('slow_sql_summary', [])
        trace_results = all_results.get('trace_results', [])
        optimizer_results = all_results.get('optimizer_results', [])
        tkprof_results = all_results.get('tkprof_results', [])
        
        report_data['summary'] = {
            'total_slow_sqls': len(slow_sqls),
            'traced_sqls': len(trace_results),
            'optimizer_analyzed': len(optimizer_results),
            'tkprof_analyzed': len(tkprof_results),
            'critical_issues': 0,
            'warnings': 0
        }
        
        # 2. 느린 SQL 분석
        report_data['slow_sql_analysis'] = self._analyze_slow_sqls(slow_sqls)
        
        # 3. 트레이스 분석
        report_data['trace_analysis'] = self._analyze_trace_results(trace_results, tkprof_results)
        
        # 4. 10053 옵티마이저 분석
        report_data['optimizer_analysis'] = self._analyze_optimizer_results(optimizer_results)
        
        # 5. 권고사항 생성
        report_data['recommendations'] = self._generate_recommendations(report_data)
        
        # 6. 통계 정보
        report_data['statistics'] = self._calculate_statistics(all_results)
        
        return report_data
    
    def _analyze_slow_sqls(self, slow_sqls: List[Dict[str, Any]]) -> Dict[str, Any]:
        """느린 SQL 분석"""
        if not slow_sqls:
            return {'top_sqls': [], 'patterns': [], 'statistics': {}}
        
        # 상위 10개 SQL
        top_sqls = sorted(slow_sqls, key=lambda x: x.get('elapsed_time_ms', 0), reverse=True)[:10]
        
        # 패턴 분석
        patterns = {
            'high_cpu': len([sql for sql in slow_sqls if sql.get('cpu_time_ms', 0) > 5000]),
            'high_io': len([sql for sql in slow_sqls if sql.get('buffer_gets', 0) > 100000]),
            'frequent_exec': len([sql for sql in slow_sqls if sql.get('executions', 0) > 100]),
            'parse_intensive': len([sql for sql in slow_sqls if sql.get('parse_calls', 0) > sql.get('executions', 0) * 0.5])
        }
        
        # 통계
        total_elapsed = sum(sql.get('elapsed_time_ms', 0) for sql in slow_sqls)
        total_cpu = sum(sql.get('cpu_time_ms', 0) for sql in slow_sqls)
        
        statistics = {
            'total_elapsed_time_sec': total_elapsed / 1000,
            'total_cpu_time_sec': total_cpu / 1000,
            'avg_elapsed_time_ms': total_elapsed / len(slow_sqls) if slow_sqls else 0,
            'cpu_utilization_pct': (total_cpu / total_elapsed * 100) if total_elapsed > 0 else 0
        }
        
        return {
            'top_sqls': top_sqls,
            'patterns': patterns,
            'statistics': statistics
        }
    
    def _analyze_trace_results(self, trace_results: List[Dict[str, Any]], 
                             tkprof_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """트레이스 분석"""
        analysis = {
            'successful_traces': len(trace_results),
            'failed_traces': 0,  # 실패 추적은 별도 구현 필요
            'tkprof_analyses': [],
            'common_issues': []
        }
        
        # tkprof 결과 분석
        for tkprof in tkprof_results:
            if tkprof.get('parsed_data'):
                parsed = tkprof['parsed_data']
                
                # 주요 이슈 추출
                issues = []
                for sql_stat in parsed.get('sql_statements', []):
                    if sql_stat.get('parse_count', 0) > sql_stat.get('execute_count', 0) * 0.3:
                        issues.append('높은 파싱 비율')
                    
                    if sql_stat.get('rows_per_fetch', 0) > 1000:
                        issues.append('비효율적인 Fetch')
                
                analysis['tkprof_analyses'].append({
                    'sql_id': tkprof.get('sql_id', 'Unknown'),
                    'total_calls': parsed.get('total_calls', 0),
                    'total_elapsed': parsed.get('total_elapsed', 0),
                    'issues': issues
                })
        
        return analysis
    
    def _analyze_optimizer_results(self, optimizer_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """10053 옵티마이저 분석 결과 처리"""
        if not optimizer_results:
            return {
                'analyzed_count': 0,
                'summaries': [],
                'common_issues': [],
                'cost_statistics': {},
                'parameter_analysis': {}
            }
        
        analysis = {
            'analyzed_count': len(optimizer_results),
            'summaries': [],
            'common_issues': [],
            'cost_statistics': {},
            'parameter_analysis': {}
        }
        
        all_issues = []
        cost_differences = []
        all_parameters = {}
        
        # 각 결과 분석
        for result in optimizer_results:
            parsed_data = result.get('parsed_data', {})
            
            # 개별 요약
            summary = {
                'sql_id': result.get('sql_id', 'Unknown'),
                'analysis_time': result.get('analysis_time'),
                'html_report': result.get('html_report'),
                'issues_count': len(parsed_data.get('issues', [])),
                'tables_analyzed': len(parsed_data.get('base_statistics', {})),
                'join_orders_considered': len(parsed_data.get('join_orders', [])),
                'best_cost': parsed_data.get('best_join_order', {}).get('cost', 0),
                'cost_difference_pct': parsed_data.get('cost_analysis', {}).get('cost_difference_pct', 0)
            }
            
            # 주요 이슈 요약
            main_issues = []
            for issue in parsed_data.get('issues', [])[:3]:  # 상위 3개
                main_issues.append(f"{issue['type']}: {issue['message']}")
            summary['main_issues'] = main_issues
            
            analysis['summaries'].append(summary)
            
            # 전체 이슈 수집
            all_issues.extend(parsed_data.get('issues', []))
            
            # 비용 차이 수집
            cost_analysis = parsed_data.get('cost_analysis', {})
            if cost_analysis.get('cost_difference_pct'):
                cost_differences.append(cost_analysis['cost_difference_pct'])
            
            # 옵티마이저 파라미터 수집
            params = parsed_data.get('optimizer_parameters', {})
            for param, value in params.items():
                if param not in all_parameters:
                    all_parameters[param] = []
                all_parameters[param].append(value)
        
        # 공통 이슈 분석
        issue_types = {}
        for issue in all_issues:
            issue_type = issue['type']
            if issue_type not in issue_types:
                issue_types[issue_type] = 0
            issue_types[issue_type] += 1
        
        analysis['common_issues'] = [
            {'type': issue_type, 'count': count, 'percentage': count / len(optimizer_results) * 100}
            for issue_type, count in sorted(issue_types.items(), key=lambda x: x[1], reverse=True)
        ]
        
        # 비용 통계
        if cost_differences:
            analysis['cost_statistics'] = {
                'avg_cost_difference': sum(cost_differences) / len(cost_differences),
                'max_cost_difference': max(cost_differences),
                'min_cost_difference': min(cost_differences),
                'high_difference_count': len([diff for diff in cost_differences if diff > 50])
            }
        
        # 파라미터 분석 (일관성 체크)
        param_analysis = {}
        for param, values in all_parameters.items():
            unique_values = set(values)
            param_analysis[param] = {
                'unique_values': len(unique_values),
                'most_common': max(unique_values, key=values.count) if unique_values else None,
                'is_consistent': len(unique_values) == 1
            }
        
        analysis['parameter_analysis'] = param_analysis
        
        return analysis
    
    def _generate_recommendations(self, report_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """권고사항 생성"""
        recommendations = []
        
        # 느린 SQL 기반 권고사항
        slow_sql_analysis = report_data['slow_sql_analysis']
        if slow_sql_analysis.get('patterns', {}).get('high_cpu', 0) > 0:
            recommendations.append({
                'category': 'CPU 사용률',
                'priority': 'HIGH',
                'title': 'CPU 집약적 SQL 최적화 필요',
                'description': f"{slow_sql_analysis['patterns']['high_cpu']}개의 SQL에서 높은 CPU 사용률 감지",
                'actions': [
                    '인덱스 추가 검토',
                    '조인 방식 최적화',
                    '불필요한 함수 호출 제거'
                ]
            })
        
        if slow_sql_analysis.get('patterns', {}).get('parse_intensive', 0) > 0:
            recommendations.append({
                'category': '파싱 효율성',
                'priority': 'MEDIUM',
                'title': '과도한 파싱 최적화 필요',
                'description': f"{slow_sql_analysis['patterns']['parse_intensive']}개의 SQL에서 높은 파싱 비율 감지",
                'actions': [
                    '바인드 변수 사용 확대',
                    '커서 공유 최적화',
                    'SQL 표준화'
                ]
            })
        
        # 10053 분석 기반 권고사항
        optimizer_analysis = report_data['optimizer_analysis']
        for issue in optimizer_analysis.get('common_issues', []):
            if issue['percentage'] > 50:  # 50% 이상에서 발생하는 이슈
                if issue['type'] == 'COST_DIFFERENCE':
                    recommendations.append({
                        'category': '옵티마이저 성능',
                        'priority': 'MEDIUM',
                        'title': '조인 순서 최적화 검토 필요',
                        'description': f"{issue['percentage']:.1f}%의 SQL에서 조인 순서별 큰 비용 차이 발견",
                        'actions': [
                            '테이블 통계 정보 갱신',
                            '조인 힌트 사용 검토',
                            '인덱스 전략 재검토'
                        ]
                    })
                
                elif issue['type'] == 'FULL_TABLE_SCAN':
                    recommendations.append({
                        'category': '인덱스 전략',
                        'priority': 'LOW',
                        'title': 'Full Table Scan 검토',
                        'description': f"{issue['percentage']:.1f}%의 SQL에서 Full Table Scan이 최적으로 선택됨",
                        'actions': [
                            '데이터 분포 분석',
                            '인덱스 선택성 검토',
                            '파티셔닝 고려'
                        ]
                    })
        
        # 비용 차이 기반 권고사항
        cost_stats = optimizer_analysis.get('cost_statistics', {})
        if cost_stats.get('avg_cost_difference', 0) > 30:
            recommendations.append({
                'category': '통계 정보',
                'priority': 'HIGH',
                'title': '통계 정보 갱신 필요',
                'description': f"평균 비용 차이 {cost_stats['avg_cost_difference']:.1f}% 로 높음",
                'actions': [
                    'DBMS_STATS로 통계 갱신',
                    '히스토그램 수집 검토',
                    '자동 통계 수집 설정 확인'
                ]
            })
        
        # 우선순위별 정렬
        priority_order = {'HIGH': 3, 'MEDIUM': 2, 'LOW': 1}
        recommendations.sort(key=lambda x: priority_order.get(x['priority'], 0), reverse=True)
        
        return recommendations
    
    def _calculate_statistics(self, all_results: Dict[str, Any]) -> Dict[str, Any]:
        """전체 통계 계산"""
        slow_sqls = all_results.get('slow_sql_summary', [])
        optimizer_results = all_results.get('optimizer_results', [])
        
        total_elapsed = sum(sql.get('elapsed_time_ms', 0) for sql in slow_sqls)
        
        statistics = {
            'analysis_period': '24 hours',  # 설정에서 가져와야 함
            'total_sqls_analyzed': len(slow_sqls),
            'total_elapsed_time_hours': total_elapsed / (1000 * 60 * 60),
            'optimizer_traces_collected': len(optimizer_results),
            'avg_cost_improvement_potential': 0  # 계산 로직 추가 필요
        }
        
        # 비용 개선 가능성 추정
        if optimizer_results:
            cost_differences = []
            for result in optimizer_results:
                cost_analysis = result.get('parsed_data', {}).get('cost_analysis', {})
                if cost_analysis.get('cost_difference_pct'):
                    cost_differences.append(cost_analysis['cost_difference_pct'])
            
            if cost_differences:
                statistics['avg_cost_improvement_potential'] = sum(cost_differences) / len(cost_differences)
        
        return statistics
    
    def _generate_html_report(self, report_data: Dict[str, Any]) -> str:
        """HTML 보고서 생성"""
        html_template = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Oracle SQL 튜닝 종합 보고서</title>
    <style>
        body { 
            font-family: 'Segoe UI', Arial, sans-serif; 
            margin: 20px; 
            background-color: #f8f9fa;
        }
        .container { 
            max-width: 1200px; 
            margin: 0 auto; 
            background-color: white; 
            padding: 30px; 
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .header { 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px; 
            border-radius: 8px; 
            margin-bottom: 30px; 
            text-align: center;
        }
        .summary-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }
        .card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #667eea;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }
        .card h3 {
            margin: 0 0 10px 0;
            color: #333;
            font-size: 14px;
            text-transform: uppercase;
            font-weight: 600;
        }
        .card .value {
            font-size: 28px;
            font-weight: bold;
            color: #667eea;
        }
        .section { 
            margin-bottom: 40px; 
            background: white;
            padding: 25px;
            border-radius: 8px;
            border: 1px solid #e9ecef;
        }
        .section h2 { 
            color: #333; 
            border-bottom: 3px solid #667eea; 
            padding-bottom: 10px;
            margin-bottom: 20px;
        }
        table { 
            border-collapse: collapse; 
            width: 100%; 
            margin-bottom: 20px;
            background: white;
        }
        th, td { 
            border: 1px solid #dee2e6; 
            padding: 12px 8px; 
            text-align: left; 
        }
        th { 
            background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
            font-weight: 600;
            color: #495057;
        }
        tr:nth-child(even) { background-color: #f8f9fa; }
        tr:hover { background-color: #e3f2fd; }
        
        .recommendation {
            margin: 15px 0;
            padding: 15px;
            border-radius: 6px;
            border-left: 4px solid #28a745;
        }
        .recommendation.HIGH { border-left-color: #dc3545; background-color: #f8d7da; }
        .recommendation.MEDIUM { border-left-color: #ffc107; background-color: #fff3cd; }
        .recommendation.LOW { border-left-color: #28a745; background-color: #d4edda; }
        
        .recommendation h4 {
            margin: 0 0 8px 0;
            color: #333;
        }
        .recommendation .priority {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: bold;
            color: white;
        }
        .priority.HIGH { background-color: #dc3545; }
        .priority.MEDIUM { background-color: #ffc107; }
        .priority.LOW { background-color: #28a745; }
        
        .issue-summary {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }
        .issue-card {
            padding: 15px;
            border-radius: 6px;
            background: #f8f9fa;
            border-left: 4px solid #6c757d;
        }
        .optimizer-summary {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }
        .optimizer-card {
            background: #ffffff;
            border: 1px solid #dee2e6;
            border-radius: 8px;
            padding: 20px;
        }
        .optimizer-card h4 {
            margin: 0 0 15px 0;
            color: #495057;
        }
        .metric {
            display: flex;
            justify-content: space-between;
            margin: 8px 0;
            padding: 5px 0;
            border-bottom: 1px solid #f1f3f4;
        }
        .metric:last-child {
            border-bottom: none;
        }
        .metric .label {
            font-weight: 500;
            color: #6c757d;
        }
        .metric .value {
            font-weight: bold;
            color: #333;
        }
        .footer {
            text-align: center;
            padding: 20px;
            color: #6c757d;
            font-size: 14px;
            border-top: 1px solid #dee2e6;
            margin-top: 30px;
        }
        pre {
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 6px;
            overflow-x: auto;
            border: 1px solid #e9ecef;
        }
        .highlight { 
            background-color: #fff3cd; 
            padding: 2px 4px;
            border-radius: 3px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Oracle SQL 튜닝 종합 보고서</h1>
            <p>생성 시간: {{ report_data.generation_time.strftime('%Y-%m-%d %H:%M:%S') }}</p>
        </div>

        <!-- 요약 카드 -->
        <div class="summary-cards">
            <div class="card">
                <h3>감지된 느린 SQL</h3>
                <div class="value">{{ report_data.summary.total_slow_sqls }}</div>
            </div>
            <div class="card">
                <h3>트레이스 수집</h3>
                <div class="value">{{ report_data.summary.traced_sqls }}</div>
            </div>
            <div class="card">
                <h3>옵티마이저 분석</h3>
                <div class="value">{{ report_data.summary.optimizer_analyzed }}</div>
            </div>
            <div class="card">
                <h3>tkprof 분석</h3>
                <div class="value">{{ report_data.summary.tkprof_analyzed }}</div>
            </div>
        </div>

        <!-- 권고사항 -->
        {% if report_data.recommendations %}
        <div class="section">
            <h2>🎯 주요 권고사항</h2>
            {% for rec in report_data.recommendations %}
            <div class="recommendation {{ rec.priority }}">
                <h4>
                    {{ rec.title }}
                    <span class="priority {{ rec.priority }}">{{ rec.priority }}</span>
                </h4>
                <p><strong>카테고리:</strong> {{ rec.category }}</p>
                <p>{{ rec.description }}</p>
                <strong>권장 조치:</strong>
                <ul>
                    {% for action in rec.actions %}
                    <li>{{ action }}</li>
                    {% endfor %}
                </ul>
            </div>
            {% endfor %}
        </div>
        {% endif %}

        <!-- 느린 SQL 분석 -->
        {% if report_data.slow_sql_analysis.top_sqls %}
        <div class="section">
            <h2>🐌 느린 SQL 분석</h2>
            
            <h3>상위 느린 SQL (Top 10)</h3>
            <table>
                <tr>
                    <th>순위</th>
                    <th>SQL_ID</th>
                    <th>실행 시간 (ms)</th>
                    <th>CPU 시간 (ms)</th>
                    <th>실행 횟수</th>
                    <th>Buffer Gets</th>
                </tr>
                {% for sql in report_data.slow_sql_analysis.top_sqls %}
                <tr>
                    <td>{{ loop.index }}</td>
                    <td>{{ sql.sql_id }}</td>
                    <td>{{ "{:,}".format(sql.elapsed_time_ms) }}</td>
                    <td>{{ "{:,}".format(sql.get('cpu_time_ms', 0)) }}</td>
                    <td>{{ "{:,}".format(sql.get('executions', 0)) }}</td>
                    <td>{{ "{:,}".format(sql.get('buffer_gets', 0)) }}</td>
                </tr>
                {% endfor %}
            </table>

            <h3>패턴 분석</h3>
            <div class="issue-summary">
                <div class="issue-card">
                    <h4>높은 CPU 사용</h4>
                    <p>{{ report_data.slow_sql_analysis.patterns.high_cpu }}개 SQL</p>
                </div>
                <div class="issue-card">
                    <h4>높은 I/O</h4>
                    <p>{{ report_data.slow_sql_analysis.patterns.high_io }}개 SQL</p>
                </div>
                <div class="issue-card">
                    <h4>빈번한 실행</h4>
                    <p>{{ report_data.slow_sql_analysis.patterns.frequent_exec }}개 SQL</p>
                </div>
                <div class="issue-card">
                    <h4>과도한 파싱</h4>
                    <p>{{ report_data.slow_sql_analysis.patterns.parse_intensive }}개 SQL</p>
                </div>
            </div>
        </div>
        {% endif %}

        <!-- 10053 옵티마이저 분석 -->
        {% if report_data.optimizer_analysis.analyzed_count > 0 %}
        <div class="section">
            <h2>🧠 옵티마이저 분석 (10053 트레이스)</h2>
            
            <h3>분석 요약</h3>
            <p><strong>분석된 SQL 수:</strong> {{ report_data.optimizer_analysis.analyzed_count }}개</p>
            
            {% if report_data.optimizer_analysis.cost_statistics %}
            <div class="card">
                <h3>비용 분석 통계</h3>
                <div class="metric">
                    <span class="label">평균 비용 차이:</span>
                    <span class="value">{{ "%.1f"|format(report_data.optimizer_analysis.cost_statistics.avg_cost_difference) }}%</span>
                </div>
                <div class="metric">
                    <span class="label">최대 비용 차이:</span>
                    <span class="value">{{ "%.1f"|format(report_data.optimizer_analysis.cost_statistics.max_cost_difference) }}%</span>
                </div>
                <div class="metric">
                    <span class="label">높은 차이 (>50%) SQL:</span>
                    <span class="value">{{ report_data.optimizer_analysis.cost_statistics.high_difference_count }}개</span>
                </div>
            </div>
            {% endif %}

            <h3>개별 SQL 분석 결과</h3>
            <div class="optimizer-summary">
                {% for summary in report_data.optimizer_analysis.summaries %}
                <div class="optimizer-card">
                    <h4>SQL_ID: {{ summary.sql_id }}</h4>
                    <div class="metric">
                        <span class="label">이슈 수:</span>
                        <span class="value">{{ summary.issues_count }}개</span>
                    </div>
                    <div class="metric">
                        <span class="label">분석 테이블:</span>
                        <span class="value">{{ summary.tables_analyzed }}개</span>
                    </div>
                    <div class="metric">
                        <span class="label">조인 순서 후보:</span>
                        <span class="value">{{ summary.join_orders_considered }}개</span>
                    </div>
                    <div class="metric">
                        <span class="label">최적 비용:</span>
                        <span class="value">{{ "{:,}".format(summary.best_cost) }}</span>
                    </div>
                    <div class="metric">
                        <span class="label">비용 차이:</span>
                        <span class="value">{{ "%.1f"|format(summary.cost_difference_pct) }}%</span>
                    </div>
                    {% if summary.main_issues %}
                    <div style="margin-top: 15px;">
                        <strong>주요 이슈:</strong>
                        <ul style="margin: 5px 0; padding-left: 20px; font-size: 14px;">
                            {% for issue in summary.main_issues %}
                            <li>{{ issue }}</li>
                            {% endfor %}
                        </ul>
                    </div>
                    {% endif %}
                    {% if summary.html_report %}
                    <p style="margin-top: 10px;">
                        <a href="{{ summary.html_report }}" target="_blank">상세 리포트 보기</a>
                    </p>
                    {% endif %}
                </div>
                {% endfor %}
            </div>

            {% if report_data.optimizer_analysis.common_issues %}
            <h3>공통 이슈 패턴</h3>
            <table>
                <tr>
                    <th>이슈 유형</th>
                    <th>발생 횟수</th>
                    <th>비율</th>
                </tr>
                {% for issue in report_data.optimizer_analysis.common_issues %}
                <tr>
                    <td>{{ issue.type }}</td>
                    <td>{{ issue.count }}회</td>
                    <td>{{ "%.1f"|format(issue.percentage) }}%</td>
                </tr>
                {% endfor %}
            </table>
            {% endif %}
        </div>
        {% endif %}

        <!-- 전체 통계 -->
        <div class="section">
            <h2>📊 전체 통계</h2>
            <table>
                <tr>
                    <th>항목</th>
                    <th>값</th>
                </tr>
                <tr>
                    <td>분석 기간</td>
                    <td>{{ report_data.statistics.analysis_period }}</td>
                </tr>
                <tr>
                    <td>총 분석 SQL 수</td>
                    <td>{{ "{:,}".format(report_data.statistics.total_sqls_analyzed) }}</td>
                </tr>
                <tr>
                    <td>총 실행 시간</td>
                    <td>{{ "%.1f"|format(report_data.statistics.total_elapsed_time_hours) }} 시간</td>
                </tr>
                <tr>
                    <td>옵티마이저 트레이스 수집</td>
                    <td>{{ report_data.statistics.optimizer_traces_collected }}개</td>
                </tr>
                {% if report_data.statistics.avg_cost_improvement_potential > 0 %}
                <tr>
                    <td>평균 비용 개선 잠재율</td>
                    <td>{{ "%.1f"|format(report_data.statistics.avg_cost_improvement_potential) }}%</td>
                </tr>
                {% endif %}
            </table>
        </div>

        <div class="footer">
            <p>Oracle SQL 튜닝 자동화 파이프라인 | 10053 옵티마이저 트레이스 포함</p>
            <p>생성된 리포트는 정기적으로 검토하여 지속적인 성능 최적화에 활용하시기 바랍니다.</p>
        </div>
    </div>
</body>
</html>
        """
        
        from jinja2 import Template
        template = Template(html_template)
        return template.render(report_data=report_data)