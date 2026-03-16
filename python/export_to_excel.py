#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
엑셀 내보내기 모듈 - 10053 옵티마이저 분석 시트 포함
"""

import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.chart import BarChart, Reference
from openpyxl.formatting.rule import CellIsRule


class ExcelExporter:
    """엑셀 내보내기 클래스"""
    
    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger
        self.output_dir = os.path.join(
            config['output']['base_directory'],
            config['output']['directories']['excel']
        )
        
        # 스타일 정의
        self.header_font = Font(bold=True, color="FFFFFF")
        self.header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        self.header_alignment = Alignment(horizontal="center", vertical="center")
        
        self.border_thin = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
    def export_to_excel(self, all_results: Dict[str, Any]) -> str:
        """전체 결과를 엑셀 파일로 내보내기"""
        self.logger.info("엑셀 내보내기 시작")
        
        try:
            # 파일명 생성
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename_format = self.config.get('excel_export', {}).get('filename_format', 
                                                                    'SQL_Tuning_Report_{timestamp}.xlsx')
            filename = filename_format.format(timestamp=timestamp)
            filepath = os.path.join(self.output_dir, filename)
            
            os.makedirs(self.output_dir, exist_ok=True)
            
            # Workbook 생성
            wb = Workbook()
            wb.remove(wb.active)  # 기본 시트 삭제
            
            # 각 시트 생성
            self._create_summary_sheet(wb, all_results)
            self._create_slow_sql_sheet(wb, all_results.get('slow_sql_summary', []))
            self._create_trace_analysis_sheet(wb, all_results.get('trace_results', []), 
                                            all_results.get('tkprof_results', []))
            self._create_optimizer_decisions_sheet(wb, all_results.get('optimizer_results', []))
            self._create_recommendations_sheet(wb, all_results)
            
            # 파일 저장
            wb.save(filepath)
            
            self.logger.info(f"엑셀 내보내기 완료: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"엑셀 내보내기 실패: {e}")
            return None
    
    def _create_summary_sheet(self, wb: Workbook, all_results: Dict[str, Any]):
        """요약 시트 생성"""
        ws = wb.create_sheet("요약", 0)
        
        # 헤더
        ws['A1'] = 'Oracle SQL 튜닝 분석 요약'
        ws['A1'].font = Font(size=16, bold=True, color="2F5597")
        ws.merge_cells('A1:D1')
        
        ws['A2'] = f"생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ws.merge_cells('A2:D2')
        
        # 요약 통계
        summary_data = [
            ['구분', '개수', '비고', '상태'],
            ['감지된 느린 SQL', len(all_results.get('slow_sql_summary', [])), '분석 대상', '✓'],
            ['10046 트레이스 수집', len(all_results.get('trace_results', [])), '실행 추적', '✓'],
            ['10053 옵티마이저 분석', len(all_results.get('optimizer_results', [])), '실행계획 분석', '✓'],
            ['tkprof 분석', len(all_results.get('tkprof_results', [])), '성능 분석', '✓'],
            ['생성된 권고사항', 0, '개선 방안', '✓']  # 계산 필요
        ]
        
        # 권고사항 수 계산
        if 'slow_sql_summary' in all_results:
            from python.report_generator import ReportGenerator
            temp_generator = ReportGenerator(self.config, self.logger)
            temp_data = temp_generator._prepare_report_data(all_results)
            summary_data[5][1] = len(temp_data.get('recommendations', []))
        
        # 데이터 입력
        for row_idx, row_data in enumerate(summary_data, start=4):
            for col_idx, value in enumerate(row_data, start=1):
                cell = ws.cell(row=row_idx, column=col_idx, value=value)
                if row_idx == 4:  # 헤더 행
                    cell.font = self.header_font
                    cell.fill = self.header_fill
                    cell.alignment = self.header_alignment
                cell.border = self.border_thin
        
        # 열 너비 조정
        ws.column_dimensions['A'].width = 20
        ws.column_dimensions['B'].width = 15
        ws.column_dimensions['C'].width = 20
        ws.column_dimensions['D'].width = 10
        
        # 옵티마이저 분석 요약 (10053)
        if all_results.get('optimizer_results'):
            ws['A11'] = '10053 옵티마이저 분석 요약'
            ws['A11'].font = Font(size=14, bold=True, color="2F5597")
            ws.merge_cells('A11:D11')
            
            optimizer_results = all_results['optimizer_results']
            
            # 통계 계산
            total_issues = sum(len(result.get('parsed_data', {}).get('issues', [])) 
                             for result in optimizer_results)
            
            cost_differences = []
            for result in optimizer_results:
                cost_analysis = result.get('parsed_data', {}).get('cost_analysis', {})
                if cost_analysis.get('cost_difference_pct'):
                    cost_differences.append(cost_analysis['cost_difference_pct'])
            
            avg_cost_diff = sum(cost_differences) / len(cost_differences) if cost_differences else 0
            
            optimizer_summary = [
                ['항목', '값', '단위', '상태'],
                ['분석 완료 SQL', len(optimizer_results), '개', '✓'],
                ['발견된 총 이슈', total_issues, '개', '⚠️' if total_issues > 0 else '✓'],
                ['평균 비용 차이', f'{avg_cost_diff:.1f}', '%', '⚠️' if avg_cost_diff > 30 else '✓'],
                ['비용 차이 큰 SQL (>50%)', len([d for d in cost_differences if d > 50]), '개', '⚠️' if any(d > 50 for d in cost_differences) else '✓']
            ]
            
            for row_idx, row_data in enumerate(optimizer_summary, start=13):
                for col_idx, value in enumerate(row_data, start=1):
                    cell = ws.cell(row=row_idx, column=col_idx, value=value)
                    if row_idx == 13:  # 헤더 행
                        cell.font = self.header_font
                        cell.fill = self.header_fill
                        cell.alignment = self.header_alignment
                    cell.border = self.border_thin
    
    def _create_slow_sql_sheet(self, wb: Workbook, slow_sqls: List[Dict[str, Any]]):
        """느린 SQL 시트 생성"""
        ws = wb.create_sheet("느린 SQL 요약")
        
        if not slow_sqls:
            ws['A1'] = '감지된 느린 SQL이 없습니다.'
            return
        
        # 데이터프레임 생성
        df_data = []
        for idx, sql in enumerate(slow_sqls, 1):
            df_data.append({
                '순위': idx,
                'SQL_ID': sql.get('sql_id', ''),
                '실행시간(ms)': sql.get('elapsed_time_ms', 0),
                'CPU시간(ms)': sql.get('cpu_time_ms', 0),
                '실행횟수': sql.get('executions', 0),
                'Buffer Gets': sql.get('buffer_gets', 0),
                '평균 실행시간(ms)': sql.get('elapsed_time_ms', 0) / max(sql.get('executions', 1), 1),
                'Parse Calls': sql.get('parse_calls', 0),
                'Parse/Exec 비율': sql.get('parse_calls', 0) / max(sql.get('executions', 1), 1),
                'First Load Time': sql.get('first_load_time', ''),
                'SQL Text (50자)': (sql.get('sql_text', '')[:50] + '...') if len(sql.get('sql_text', '')) > 50 else sql.get('sql_text', '')
            })
        
        df = pd.DataFrame(df_data)
        
        # 데이터프레임을 시트에 추가
        for r in dataframe_to_rows(df, index=False, header=True):
            ws.append(r)
        
        # 헤더 스타일 적용
        for cell in ws[1]:
            cell.font = self.header_font
            cell.fill = self.header_fill
            cell.alignment = self.header_alignment
            cell.border = self.border_thin
        
        # 데이터 셀 스타일 적용
        for row in ws.iter_rows(min_row=2, max_row=len(df)+1):
            for cell in row:
                cell.border = self.border_thin
        
        # 조건부 서식 적용
        if len(df) > 0:
            # 실행시간 기준 색상 조건부 서식
            elapsed_range = f'C2:C{len(df)+1}'
            ws.conditional_formatting.add(elapsed_range, 
                CellIsRule(operator='greaterThan', formula=['10000'], 
                          fill=PatternFill(start_color='FFD7D7', end_color='FFD7D7')))
            
            # Parse/Exec 비율 기준 색상 조건부 서식  
            parse_range = f'I2:I{len(df)+1}'
            ws.conditional_formatting.add(parse_range,
                CellIsRule(operator='greaterThan', formula=['0.3'],
                          fill=PatternFill(start_color='FFF2CC', end_color='FFF2CC')))
        
        # 자동 필터 추가
        ws.auto_filter.ref = f'A1:K{len(df)+1}'
        
        # 첫 행 고정
        ws.freeze_panes = 'A2'
        
        # 열 너비 자동 조정
        for column in ws.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws.column_dimensions[column_letter].width = adjusted_width
    
    def _create_trace_analysis_sheet(self, wb: Workbook, trace_results: List[Dict[str, Any]], 
                                   tkprof_results: List[Dict[str, Any]]):
        """트레이스 분석 시트 생성"""
        ws = wb.create_sheet("트레이스 분석")
        
        if not trace_results:
            ws['A1'] = '트레이스 분석 결과가 없습니다.'
            return
        
        # tkprof 결과를 SQL_ID별로 매핑
        tkprof_by_sql = {}
        for tkprof in tkprof_results:
            sql_id = tkprof.get('sql_id')
            if sql_id:
                tkprof_by_sql[sql_id] = tkprof
        
        # 데이터프레임 생성
        df_data = []
        for trace in trace_results:
            sql_id = trace.get('sql_id', '')
            tkprof_data = tkprof_by_sql.get(sql_id, {}).get('parsed_data', {})
            
            # tkprof 통계에서 주요 정보 추출
            total_calls = 0
            total_elapsed = 0
            total_cpu = 0
            total_disk = 0
            
            if tkprof_data.get('sql_statements'):
                for stmt in tkprof_data['sql_statements']:
                    total_calls += stmt.get('execute_count', 0)
                    total_elapsed += stmt.get('total_elapsed', 0)
                    total_cpu += stmt.get('cpu_time', 0)
                    total_disk += stmt.get('disk_reads', 0)
            
            df_data.append({
                'SQL_ID': sql_id,
                '트레이스 파일': os.path.basename(trace.get('trace_file', '')),
                '수집 시간': trace.get('collection_time', '').strftime('%Y-%m-%d %H:%M:%S') if trace.get('collection_time') else '',
                'tkprof 분석': 'Y' if sql_id in tkprof_by_sql else 'N',
                '총 실행 횟수': total_calls,
                '총 경과시간(초)': round(total_elapsed, 2),
                '총 CPU시간(초)': round(total_cpu, 2),
                '총 디스크 읽기': total_disk,
                '평균 실행시간(ms)': round((total_elapsed * 1000) / max(total_calls, 1), 2),
                'CPU 사용률(%)': round((total_cpu / max(total_elapsed, 0.001)) * 100, 1),
                '주요 이슈': self._extract_trace_issues(tkprof_data)
            })
        
        df = pd.DataFrame(df_data)
        
        # 데이터프레임을 시트에 추가
        for r in dataframe_to_rows(df, index=False, header=True):
            ws.append(r)
        
        # 헤더 스타일 적용
        for cell in ws[1]:
            cell.font = self.header_font
            cell.fill = self.header_fill
            cell.alignment = self.header_alignment
            cell.border = self.border_thin
        
        # 데이터 셀 스타일 적용
        for row in ws.iter_rows(min_row=2, max_row=len(df)+1):
            for cell in row:
                cell.border = self.border_thin
        
        # 조건부 서식
        if len(df) > 0:
            # CPU 사용률 기준
            cpu_range = f'J2:J{len(df)+1}'
            ws.conditional_formatting.add(cpu_range,
                CellIsRule(operator='greaterThan', formula=['80'],
                          fill=PatternFill(start_color='FFD7D7', end_color='FFD7D7')))
        
        # 자동 필터 및 고정창
        ws.auto_filter.ref = f'A1:K{len(df)+1}'
        ws.freeze_panes = 'A2'
        
        # 열 너비 조정
        for column in ws.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 40)
            ws.column_dimensions[column_letter].width = adjusted_width
    
    def _create_optimizer_decisions_sheet(self, wb: Workbook, optimizer_results: List[Dict[str, Any]]):
        """옵티마이저 결정 시트 생성 (10053 분석)"""
        ws = wb.create_sheet("옵티마이저 결정")
        
        if not optimizer_results:
            ws['A1'] = '10053 옵티마이저 분석 결과가 없습니다.'
            ws['A3'] = '분석된 SQL이 없습니다. main.py run --with-10053 옵션을 사용하여 10053 트레이스를 수집하세요.'
            return
        
        # 1. 요약 정보
        ws['A1'] = '10053 옵티마이저 분석 요약'
        ws['A1'].font = Font(size=14, bold=True, color="2F5597")
        ws.merge_cells('A1:F1')
        
        # 요약 통계
        total_analyzed = len(optimizer_results)
        total_issues = sum(len(result.get('parsed_data', {}).get('issues', [])) for result in optimizer_results)
        
        cost_differences = []
        high_cost_diff_count = 0
        
        for result in optimizer_results:
            cost_analysis = result.get('parsed_data', {}).get('cost_analysis', {})
            if cost_analysis.get('cost_difference_pct'):
                diff = cost_analysis['cost_difference_pct']
                cost_differences.append(diff)
                if diff > 50:
                    high_cost_diff_count += 1
        
        avg_cost_diff = sum(cost_differences) / len(cost_differences) if cost_differences else 0
        
        # 요약 데이터 입력
        summary_data = [
            ['항목', '값', '단위', '상태', '설명'],
            ['분석 완료 SQL', total_analyzed, '개', '✓', '10053 트레이스 분석 완료'],
            ['발견된 총 이슈', total_issues, '개', '⚠️' if total_issues > 5 else '✓', '옵티마이저 관련 이슈'],
            ['평균 비용 차이', f'{avg_cost_diff:.1f}', '%', '⚠️' if avg_cost_diff > 30 else '✓', '1st vs 2nd 조인 순서'],
            ['고비용 차이 SQL (>50%)', high_cost_diff_count, '개', '⚠️' if high_cost_diff_count > 0 else '✓', '통계 갱신 필요 가능성']
        ]
        
        for row_idx, row_data in enumerate(summary_data, start=3):
            for col_idx, value in enumerate(row_data, start=1):
                cell = ws.cell(row=row_idx, column=col_idx, value=value)
                if row_idx == 3:  # 헤더 행
                    cell.font = self.header_font
                    cell.fill = self.header_fill
                    cell.alignment = self.header_alignment
                cell.border = self.border_thin
        
        # 2. 상세 분석 결과
        ws['A9'] = '개별 SQL 분석 결과'
        ws['A9'].font = Font(size=12, bold=True, color="2F5597")
        ws.merge_cells('A9:L9')
        
        # 개별 분석 데이터
        detail_data = []
        for result in optimizer_results:
            sql_id = result.get('sql_id', '')
            parsed_data = result.get('parsed_data', {})
            
            # 이슈 요약
            issues = parsed_data.get('issues', [])
            issue_summary = '; '.join([issue['type'] for issue in issues[:3]])  # 상위 3개
            
            # 테이블 정보
            base_stats = parsed_data.get('base_statistics', {})
            table_count = len(base_stats)
            total_rows = sum(stats.get('rows', 0) for stats in base_stats.values())
            
            # 조인 분석
            join_orders = parsed_data.get('join_orders', [])
            best_join = parsed_data.get('best_join_order', {})
            
            # 비용 분석
            cost_analysis = parsed_data.get('cost_analysis', {})
            
            detail_data.append({
                'SQL_ID': sql_id,
                '분석시간': result.get('analysis_time', '').strftime('%m-%d %H:%M') if result.get('analysis_time') else '',
                '이슈수': len(issues),
                '주요이슈': issue_summary[:50] + '...' if len(issue_summary) > 50 else issue_summary,
                '테이블수': table_count,
                '총행수': f'{total_rows:,}' if total_rows > 0 else '0',
                '조인순서후보': len(join_orders),
                '최적비용': f"{best_join.get('cost', 0):,}",
                '비용차이(%)': f"{cost_analysis.get('cost_difference_pct', 0):.1f}",
                '상태': '⚠️' if len(issues) > 2 or cost_analysis.get('cost_difference_pct', 0) > 30 else '✓',
                '리포트': os.path.basename(result.get('html_report', '')) if result.get('html_report') else '',
                '권장조치': self._get_optimizer_recommendation(issues, cost_analysis)
            })
        
        df_detail = pd.DataFrame(detail_data)
        
        # 상세 데이터 추가
        for r in dataframe_to_rows(df_detail, index=False, header=True):
            ws.append(r)
        
        # 상세 데이터 헤더 스타일
        header_row = len(summary_data) + 4  # 요약 + 제목 + 여백
        for cell in ws[header_row]:
            cell.font = self.header_font
            cell.fill = PatternFill(start_color="70AD47", end_color="70AD47", fill_type="solid")
            cell.alignment = self.header_alignment
            cell.border = self.border_thin
        
        # 상세 데이터 셀 스타일
        for row in ws.iter_rows(min_row=header_row+1, max_row=header_row+len(df_detail)):
            for cell in row:
                cell.border = self.border_thin
        
        # 조건부 서식
        if len(df_detail) > 0:
            # 비용 차이 기준
            cost_diff_range = f'I{header_row+1}:I{header_row+len(df_detail)}'
            ws.conditional_formatting.add(cost_diff_range,
                CellIsRule(operator='greaterThan', formula=['30'],
                          fill=PatternFill(start_color='FFD7D7', end_color='FFD7D7')))
            
            # 이슈 수 기준
            issue_range = f'C{header_row+1}:C{header_row+len(df_detail)}'
            ws.conditional_formatting.add(issue_range,
                CellIsRule(operator='greaterThan', formula=['2'],
                          fill=PatternFill(start_color='FFF2CC', end_color='FFF2CC')))
        
        # 3. 이슈 유형별 통계
        if total_issues > 0:
            ws[f'A{header_row + len(df_detail) + 3}'] = '이슈 유형별 분포'
            ws[f'A{header_row + len(df_detail) + 3}'].font = Font(size=12, bold=True, color="2F5597")
            
            # 이슈 유형 집계
            issue_types = {}
            for result in optimizer_results:
                for issue in result.get('parsed_data', {}).get('issues', []):
                    issue_type = issue['type']
                    if issue_type not in issue_types:
                        issue_types[issue_type] = 0
                    issue_types[issue_type] += 1
            
            issue_stats_data = [['이슈 유형', '발생 횟수', '비율(%)', '권장 조치']]
            for issue_type, count in sorted(issue_types.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / total_analyzed) * 100
                recommendation = self._get_issue_type_recommendation(issue_type)
                issue_stats_data.append([issue_type, count, f'{percentage:.1f}', recommendation])
            
            # 이슈 통계 데이터 추가
            issue_start_row = header_row + len(df_detail) + 5
            for row_idx, row_data in enumerate(issue_stats_data, start=issue_start_row):
                for col_idx, value in enumerate(row_data, start=1):
                    cell = ws.cell(row=row_idx, column=col_idx, value=value)
                    if row_idx == issue_start_row:  # 헤더 행
                        cell.font = self.header_font
                        cell.fill = PatternFill(start_color="E7E6E6", end_color="E7E6E6", fill_type="solid")
                        cell.alignment = self.header_alignment
                    cell.border = self.border_thin
        
        # 자동 필터
        ws.auto_filter.ref = f'A{header_row}:L{header_row+len(df_detail)}'
        
        # 첫 행 고정
        ws.freeze_panes = f'A{header_row+1}'
        
        # 열 너비 조정
        column_widths = [12, 12, 8, 25, 8, 12, 12, 12, 12, 8, 20, 30]
        for i, width in enumerate(column_widths, start=1):
            ws.column_dimensions[chr(64 + i)].width = width
    
    def _create_recommendations_sheet(self, wb: Workbook, all_results: Dict[str, Any]):
        """권고사항 시트 생성"""
        ws = wb.create_sheet("개선 권고사항")
        
        # 권고사항 생성 (ReportGenerator에서 로직 재사용)
        try:
            from python.report_generator import ReportGenerator
            temp_generator = ReportGenerator(self.config, self.logger)
            report_data = temp_generator._prepare_report_data(all_results)
            recommendations = report_data.get('recommendations', [])
        except:
            recommendations = []
        
        if not recommendations:
            ws['A1'] = '생성된 권고사항이 없습니다.'
            return
        
        # 헤더
        ws['A1'] = 'SQL 튜닝 개선 권고사항'
        ws['A1'].font = Font(size=14, bold=True, color="2F5597")
        ws.merge_cells('A1:F1')
        
        # 권고사항 데이터
        rec_data = []
        for idx, rec in enumerate(recommendations, 1):
            actions_text = '; '.join(rec.get('actions', []))
            rec_data.append({
                '순위': idx,
                '우선순위': rec.get('priority', ''),
                '카테고리': rec.get('category', ''),
                '제목': rec.get('title', ''),
                '설명': rec.get('description', ''),
                '권장조치': actions_text
            })
        
        df_rec = pd.DataFrame(rec_data)
        
        # 데이터 추가 (헤더 포함)
        for r in dataframe_to_rows(df_rec, index=False, header=True):
            ws.append(r)
        
        # 헤더 스타일
        for cell in ws[3]:  # 헤더는 3번째 행
            cell.font = self.header_font
            cell.fill = self.header_fill
            cell.alignment = self.header_alignment
            cell.border = self.border_thin
        
        # 데이터 스타일
        for row in ws.iter_rows(min_row=4, max_row=3+len(df_rec)):
            for cell in row:
                cell.border = self.border_thin
        
        # 우선순위별 색상 조건부 서식
        priority_range = f'B4:B{3+len(df_rec)}'
        
        # HIGH - 빨강
        ws.conditional_formatting.add(priority_range,
            CellIsRule(operator='equal', formula=['"HIGH"'],
                      fill=PatternFill(start_color='FFD7D7', end_color='FFD7D7')))
        
        # MEDIUM - 노랑  
        ws.conditional_formatting.add(priority_range,
            CellIsRule(operator='equal', formula=['"MEDIUM"'],
                      fill=PatternFill(start_color='FFF2CC', end_color='FFF2CC')))
        
        # LOW - 초록
        ws.conditional_formatting.add(priority_range,
            CellIsRule(operator='equal', formula=['"LOW"'],
                      fill=PatternFill(start_color='D5E8D4', end_color='D5E8D4')))
        
        # 자동 필터
        ws.auto_filter.ref = f'A3:F{3+len(df_rec)}'
        
        # 첫 행 고정
        ws.freeze_panes = 'A4'
        
        # 열 너비 조정
        ws.column_dimensions['A'].width = 8
        ws.column_dimensions['B'].width = 12
        ws.column_dimensions['C'].width = 15
        ws.column_dimensions['D'].width = 30
        ws.column_dimensions['E'].width = 40
        ws.column_dimensions['F'].width = 50
    
    def _extract_trace_issues(self, tkprof_data: Dict[str, Any]) -> str:
        """tkprof 데이터에서 주요 이슈 추출"""
        issues = []
        
        if not tkprof_data.get('sql_statements'):
            return '분석 데이터 없음'
        
        for stmt in tkprof_data['sql_statements']:
            parse_count = stmt.get('parse_count', 0)
            execute_count = stmt.get('execute_count', 0)
            
            if execute_count > 0:
                parse_ratio = parse_count / execute_count
                if parse_ratio > 0.3:
                    issues.append('높은 파싱 비율')
            
            if stmt.get('rows_per_fetch', 0) > 1000:
                issues.append('비효율적 Fetch')
            
            if stmt.get('cpu_time', 0) > 10:
                issues.append('높은 CPU 사용')
        
        return '; '.join(list(set(issues))) if issues else '정상'
    
    def _get_optimizer_recommendation(self, issues: List[Dict[str, Any]], 
                                    cost_analysis: Dict[str, Any]) -> str:
        """옵티마이저 분석 결과에 따른 권장 조치"""
        recommendations = []
        
        # 이슈별 권장사항
        for issue in issues:
            issue_type = issue.get('type', '')
            if issue_type == 'COST_DIFFERENCE':
                recommendations.append('통계갱신')
            elif issue_type == 'FULL_TABLE_SCAN':
                recommendations.append('인덱스검토')
            elif issue_type == 'STALE_STATISTICS':
                recommendations.append('통계수집')
        
        # 비용 차이 기반 권장사항
        cost_diff = cost_analysis.get('cost_difference_pct', 0)
        if cost_diff > 50:
            recommendations.append('힌트사용검토')
        elif cost_diff > 30:
            recommendations.append('통계확인')
        
        if not recommendations:
            recommendations.append('현재상태양호')
        
        return '; '.join(list(set(recommendations)))
    
    def _get_issue_type_recommendation(self, issue_type: str) -> str:
        """이슈 유형별 권장 조치"""
        recommendations = {
            'COST_DIFFERENCE': '테이블 통계 갱신, 조인 힌트 검토',
            'FULL_TABLE_SCAN': '인덱스 선택성 분석, 파티셔닝 고려',
            'STALE_STATISTICS': 'DBMS_STATS 실행, 자동 통계 설정',
            'HIGH_CPU': '인덱스 추가, SQL 재작성',
            'CARDINALITY_ERROR': '히스토그램 수집, 통계 정확성 확인'
        }
        
        return recommendations.get(issue_type, '전문가 검토 필요')