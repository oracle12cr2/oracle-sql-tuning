#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tkprof 분석 모듈 (Phase 3)
"""

import os
import subprocess
import re
from datetime import datetime
from typing import Optional, Dict, Any


class TkprofAnalyzer:
    """tkprof 분석기"""
    
    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger
        self.tkprof_config = config.get('tkprof_analysis', {})
    
    def analyze_trace(self, trace_file_path: str) -> Optional[Dict[str, Any]]:
        """tkprof로 트레이스 분석"""
        self.logger.info(f"tkprof 분석 시작: {trace_file_path}")
        
        try:
            # 출력 파일 경로
            output_dir = os.path.join(
                self.config['output']['base_directory'], 
                self.config['output']['directories']['reports']
            )
            os.makedirs(output_dir, exist_ok=True)
            
            basename = os.path.basename(trace_file_path).replace('.trc', '')
            output_file = os.path.join(output_dir, f"tkprof_{basename}.txt")
            
            # tkprof 실행
            options = self.tkprof_config.get('options', {})
            cmd = [
                'tkprof',
                trace_file_path,
                output_file,
                f"sort={options.get('sort', 'prsela')}",
                f"print={options.get('print', 20)}",
                'explain=yes' if options.get('explain', True) else 'explain=no',
                'sys=no' if not options.get('sys', False) else 'sys=yes'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                # 결과 파싱
                parsed_data = self._parse_tkprof_output(output_file)
                
                return {
                    'trace_file': trace_file_path,
                    'output_file': output_file,
                    'parsed_data': parsed_data,
                    'analysis_time': datetime.now()
                }
            else:
                self.logger.error(f"tkprof 실행 실패: {result.stderr}")
                return None
                
        except Exception as e:
            self.logger.error(f"tkprof 분석 실패: {e}")
            return None
    
    def _parse_tkprof_output(self, output_file: str) -> Dict[str, Any]:
        """tkprof 출력 파싱"""
        try:
            with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            parsed_data = {
                'sql_statements': [],
                'total_calls': 0,
                'total_elapsed': 0,
                'summary': {}
            }
            
            # SQL 문별 분석 (간단한 파싱 예시)
            sql_sections = re.split(r'\n\*{20,}\n', content)
            
            for section in sql_sections:
                if 'Parse' in section and 'Execute' in section:
                    sql_info = self._parse_sql_section(section)
                    if sql_info:
                        parsed_data['sql_statements'].append(sql_info)
                        parsed_data['total_calls'] += sql_info.get('execute_count', 0)
                        parsed_data['total_elapsed'] += sql_info.get('total_elapsed', 0)
            
            return parsed_data
            
        except Exception as e:
            self.logger.error(f"tkprof 출력 파싱 실패: {e}")
            return {}
    
    def _parse_sql_section(self, section: str) -> Optional[Dict[str, Any]]:
        """개별 SQL 섹션 파싱"""
        try:
            sql_info = {}
            
            # Parse/Execute/Fetch 정보 추출 (간단한 예시)
            parse_match = re.search(r'Parse\s+(\d+)\s+([\d.]+)\s+([\d.]+)', section)
            execute_match = re.search(r'Execute\s+(\d+)\s+([\d.]+)\s+([\d.]+)', section)
            fetch_match = re.search(r'Fetch\s+(\d+)\s+([\d.]+)\s+([\d.]+)', section)
            
            if parse_match:
                sql_info['parse_count'] = int(parse_match.group(1))
            if execute_match:
                sql_info['execute_count'] = int(execute_match.group(1))
                sql_info['total_elapsed'] = float(execute_match.group(2))
                sql_info['cpu_time'] = float(execute_match.group(3))
            if fetch_match:
                sql_info['fetch_count'] = int(fetch_match.group(1))
            
            return sql_info if sql_info else None
            
        except Exception as e:
            self.logger.error(f"SQL 섹션 파싱 실패: {e}")
            return None