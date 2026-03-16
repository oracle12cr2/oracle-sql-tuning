#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Oracle SQL 튜닝 자동화 파이프라인 - 메인 실행기
10053 옵티마이저 트레이스 기능 포함
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import List, Optional

# 프로젝트 모듈 import
sys.path.append(os.path.join(os.path.dirname(__file__), 'python'))

from python.utils import load_config, setup_logging, ensure_directories
from python.slow_sql_detector import SlowSQLDetector
from python.trace_collector import TraceCollector
from python.tkprof_analyzer import TkprofAnalyzer
from python.report_generator import ReportGenerator
from python.export_to_excel import ExcelExporter
from python.optimizer_trace import (
    collect_10053_trace, 
    analyze_10053_trace, 
    generate_10053_report,
    OptimizerTraceCollector,
    OptimizerTraceAnalyzer
)


class SQLTuningPipeline:
    """SQL 튜닝 자동화 파이프라인 메인 클래스"""
    
    def __init__(self, config_path: str = None):
        # 설정 로드
        self.config = load_config(config_path)
        
        # 로깅 설정
        self.logger = setup_logging(self.config)
        
        # 출력 디렉터리 생성
        ensure_directories(self.config)
        
        self.logger.info("SQL 튜닝 파이프라인 초기화 완료")
    
    def run_full_pipeline(self, with_10053: bool = False, sql_ids: List[str] = None):
        """전체 파이프라인 실행"""
        self.logger.info("=== SQL 튜닝 파이프라인 시작 ===")
        
        try:
            # Phase 1: 느린 SQL 감지
            slow_sql_results = self._run_phase1()
            
            if not slow_sql_results:
                self.logger.info("감지된 느린 SQL이 없습니다.")
                return
            
            # 분석 대상 SQL 결정
            target_sqls = sql_ids if sql_ids else [sql['sql_id'] for sql in slow_sql_results[:10]]  # 상위 10개
            
            all_results = {
                'slow_sql_summary': slow_sql_results,
                'trace_results': [],
                'optimizer_results': [],
                'tkprof_results': [],
                'timestamp': datetime.now()
            }
            
            # Phase 2: 10046 트레이스 수집 및 분석
            for sql_id in target_sqls:
                self.logger.info(f"Processing SQL_ID: {sql_id}")
                
                # 10046 트레이스
                trace_result = self._run_phase2(sql_id)
                if trace_result:
                    all_results['trace_results'].append(trace_result)
                    
                    # Phase 3: tkprof 분석
                    tkprof_result = self._run_phase3(trace_result['trace_file'])
                    if tkprof_result:
                        all_results['tkprof_results'].append(tkprof_result)
                
                # 10053 옵티마이저 트레이스 (선택적)
                if with_10053:
                    optimizer_result = self._run_optimizer_trace(sql_id)
                    if optimizer_result:
                        all_results['optimizer_results'].append(optimizer_result)
            
            # Phase 4: 종합 보고서 생성
            self._run_phase4(all_results)
            
            self.logger.info("=== SQL 튜닝 파이프라인 완료 ===")
            
        except Exception as e:
            self.logger.error(f"파이프라인 실행 중 오류: {e}")
            raise
    
    def _run_phase1(self):
        """Phase 1: 느린 SQL 감지"""
        self.logger.info("Phase 1: 느린 SQL 감지 시작")
        
        detector = SlowSQLDetector(self.config, self.logger)
        slow_sqls = detector.detect_slow_sqls()
        
        self.logger.info(f"Phase 1 완료: {len(slow_sqls)}개의 느린 SQL 감지")
        return slow_sqls
    
    def _run_phase2(self, sql_id: str):
        """Phase 2: 10046 트레이스 수집"""
        self.logger.info(f"Phase 2: 10046 트레이스 수집 시작 - {sql_id}")
        
        collector = TraceCollector(self.config, self.logger)
        trace_file = collector.collect_trace(sql_id)
        
        if trace_file:
            self.logger.info(f"Phase 2 완료: 트레이스 파일 생성 - {trace_file}")
            return {
                'sql_id': sql_id,
                'trace_file': trace_file,
                'collection_time': datetime.now()
            }
        else:
            self.logger.warning(f"Phase 2 실패: 트레이스 수집 실패 - {sql_id}")
            return None
    
    def _run_phase3(self, trace_file: str):
        """Phase 3: tkprof 분석"""
        self.logger.info(f"Phase 3: tkprof 분석 시작 - {trace_file}")
        
        analyzer = TkprofAnalyzer(self.config, self.logger)
        analysis_result = analyzer.analyze_trace(trace_file)
        
        if analysis_result:
            self.logger.info(f"Phase 3 완료: tkprof 분석 완료")
            return analysis_result
        else:
            self.logger.warning(f"Phase 3 실패: tkprof 분석 실패")
            return None
    
    def _run_optimizer_trace(self, sql_id: str, sql_text: str = None):
        """10053 옵티마이저 트레이스 수집 및 분석"""
        self.logger.info(f"10053 옵티마이저 트레이스 시작 - {sql_id}")
        
        try:
            # 1. 트레이스 수집
            trace_file = collect_10053_trace(sql_id, self.config, self.logger, sql_text)
            
            if not trace_file:
                self.logger.warning(f"10053 트레이스 수집 실패 - {sql_id}")
                return None
            
            # 2. 트레이스 분석
            parsed_data = analyze_10053_trace(trace_file, self.config, self.logger)
            
            # 3. HTML 리포트 생성
            output_dir = os.path.join(
                self.config['output']['base_directory'],
                self.config['output']['directories']['reports']
            )
            report_path = os.path.join(output_dir, f"10053_{sql_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
            
            html_report = generate_10053_report(parsed_data, report_path, self.config, self.logger)
            
            if html_report:
                self.logger.info(f"10053 분석 완료: {html_report}")
                return {
                    'sql_id': sql_id,
                    'trace_file': trace_file,
                    'parsed_data': parsed_data,
                    'html_report': html_report,
                    'analysis_time': datetime.now()
                }
            else:
                self.logger.warning(f"10053 리포트 생성 실패 - {sql_id}")
                return None
                
        except Exception as e:
            self.logger.error(f"10053 분석 중 오류 - {sql_id}: {e}")
            return None
    
    def _run_phase4(self, all_results):
        """Phase 4: 종합 보고서 생성"""
        self.logger.info("Phase 4: 종합 보고서 생성 시작")
        
        # HTML 리포트 생성
        report_generator = ReportGenerator(self.config, self.logger)
        html_report = report_generator.generate_comprehensive_report(all_results)
        
        # 엑셀 리포트 생성
        excel_exporter = ExcelExporter(self.config, self.logger)
        excel_file = excel_exporter.export_to_excel(all_results)
        
        self.logger.info(f"Phase 4 완료: HTML={html_report}, Excel={excel_file}")
        
        return {
            'html_report': html_report,
            'excel_file': excel_file
        }
    
    def run_optimizer_trace_only(self, sql_id: str, sql_text: str = None):
        """10053 트레이스만 단독 실행"""
        self.logger.info(f"10053 트레이스 단독 실행: {sql_id}")
        
        result = self._run_optimizer_trace(sql_id, sql_text)
        
        if result:
            self.logger.info("10053 트레이스 단독 실행 완료")
            print(f"트레이스 파일: {result['trace_file']}")
            print(f"HTML 리포트: {result['html_report']}")
            
            # 주요 결과 출력
            parsed_data = result['parsed_data']
            print(f"\n=== 분석 결과 요약 ===")
            print(f"발견된 이슈: {len(parsed_data['issues'])}개")
            
            if parsed_data['cost_analysis']:
                cost_analysis = parsed_data['cost_analysis']
                print(f"최적 비용: {cost_analysis.get('best_cost', 0):,}")
                print(f"비용 차이: {cost_analysis.get('cost_difference_pct', 0):.1f}%")
            
            if parsed_data['issues']:
                print("\n주요 이슈:")
                for issue in parsed_data['issues'][:3]:  # 상위 3개만
                    print(f"  - {issue['type']}: {issue['message']}")
        
        return result
    
    def analyze_optimizer_trace_file(self, trace_file_path: str):
        """기존 10053 트레이스 파일 분석"""
        self.logger.info(f"10053 트레이스 파일 분석: {trace_file_path}")
        
        if not os.path.exists(trace_file_path):
            self.logger.error(f"트레이스 파일을 찾을 수 없음: {trace_file_path}")
            return None
        
        try:
            # 분석
            parsed_data = analyze_10053_trace(trace_file_path, self.config, self.logger)
            
            # 리포트 생성
            basename = os.path.basename(trace_file_path)
            sql_id = basename.split('_')[1] if '_' in basename else 'unknown'
            
            output_dir = os.path.join(
                self.config['output']['base_directory'],
                self.config['output']['directories']['reports']
            )
            report_path = os.path.join(output_dir, f"10053_analysis_{sql_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
            
            html_report = generate_10053_report(parsed_data, report_path, self.config, self.logger)
            
            if html_report:
                print(f"분석 완료: {html_report}")
                
                # 결과 요약 출력
                print(f"\n=== 분석 결과 요약 ===")
                print(f"발견된 이슈: {len(parsed_data['issues'])}개")
                print(f"테이블 수: {len(parsed_data['base_statistics'])}")
                print(f"조인 순서 후보: {len(parsed_data['join_orders'])}개")
                
                return {
                    'parsed_data': parsed_data,
                    'html_report': html_report
                }
            
        except Exception as e:
            self.logger.error(f"트레이스 파일 분석 실패: {e}")
            return None


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description="Oracle SQL 튜닝 자동화 파이프라인")
    
    # 서브커맨드 설정
    subparsers = parser.add_subparsers(dest='command', help='실행할 명령')
    
    # 전체 파이프라인 실행
    run_parser = subparsers.add_parser('run', help='전체 파이프라인 실행')
    run_parser.add_argument('--with-10053', action='store_true', help='10053 옵티마이저 트레이스 포함')
    run_parser.add_argument('--sql-ids', nargs='+', help='분석할 특정 SQL_ID 목록')
    
    # 10053 트레이스 수집
    optimizer_parser = subparsers.add_parser('optimizer-trace', help='10053 옵티마이저 트레이스 수집')
    optimizer_parser.add_argument('--sql-id', required=True, help='대상 SQL_ID')
    optimizer_parser.add_argument('--sql-text', help='SQL 텍스트 (EXPLAIN PLAN용)')
    
    # 10053 트레이스 분석
    analyze_parser = subparsers.add_parser('optimizer-analyze', help='10053 트레이스 파일 분석')
    analyze_parser.add_argument('--file', required=True, help='분석할 트레이스 파일 경로')
    
    # Phase별 개별 실행
    phase1_parser = subparsers.add_parser('phase1', help='Phase 1: 느린 SQL 감지만 실행')
    
    phase2_parser = subparsers.add_parser('phase2', help='Phase 2: 트레이스 수집만 실행')
    phase2_parser.add_argument('--sql-id', required=True, help='대상 SQL_ID')
    
    phase3_parser = subparsers.add_parser('phase3', help='Phase 3: tkprof 분석만 실행')
    phase3_parser.add_argument('--trace-file', required=True, help='분석할 트레이스 파일')
    
    # 공통 옵션
    parser.add_argument('--config', help='설정 파일 경로 (기본: config/settings.yaml)')
    parser.add_argument('--verbose', '-v', action='store_true', help='상세 로그 출력')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        # 파이프라인 초기화
        pipeline = SQLTuningPipeline(args.config)
        
        # 명령어별 실행
        if args.command == 'run':
            pipeline.run_full_pipeline(
                with_10053=args.with_10053,
                sql_ids=args.sql_ids
            )
        
        elif args.command == 'optimizer-trace':
            result = pipeline.run_optimizer_trace_only(args.sql_id, args.sql_text)
            if not result:
                sys.exit(1)
        
        elif args.command == 'optimizer-analyze':
            result = pipeline.analyze_optimizer_trace_file(args.file)
            if not result:
                sys.exit(1)
        
        elif args.command == 'phase1':
            detector = SlowSQLDetector(pipeline.config, pipeline.logger)
            slow_sqls = detector.detect_slow_sqls()
            
            print(f"감지된 느린 SQL: {len(slow_sqls)}개")
            for i, sql in enumerate(slow_sqls[:10], 1):  # 상위 10개만 출력
                print(f"{i}. SQL_ID: {sql['sql_id']}, Elapsed: {sql['elapsed_time_ms']}ms")
        
        elif args.command == 'phase2':
            collector = TraceCollector(pipeline.config, pipeline.logger)
            trace_file = collector.collect_trace(args.sql_id)
            
            if trace_file:
                print(f"트레이스 수집 완료: {trace_file}")
            else:
                print("트레이스 수집 실패")
                sys.exit(1)
        
        elif args.command == 'phase3':
            analyzer = TkprofAnalyzer(pipeline.config, pipeline.logger)
            result = analyzer.analyze_trace(args.trace_file)
            
            if result:
                print(f"tkprof 분석 완료: {result.get('output_file', 'N/A')}")
            else:
                print("tkprof 분석 실패")
                sys.exit(1)
        
        print("실행 완료")
        
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단됨")
        sys.exit(1)
    except Exception as e:
        print(f"실행 중 오류 발생: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()