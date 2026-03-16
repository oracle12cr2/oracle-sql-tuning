#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
느린 SQL 감지 모듈 (Phase 1)
"""

import oracledb
from datetime import datetime, timedelta
from typing import List, Dict, Any

from utils import get_oracle_connection


class SlowSQLDetector:
    """느린 SQL 감지기"""
    
    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger
        self.detection_config = config.get('slow_sql_detection', {})
    
    def detect_slow_sqls(self) -> List[Dict[str, Any]]:
        """느린 SQL 감지"""
        self.logger.info("느린 SQL 감지 시작")
        
        try:
            conn = get_oracle_connection(self.config)
            cursor = conn.cursor()
            
            # V$SQL에서 느린 SQL 조회
            sql = """
                SELECT sql_id, sql_text, elapsed_time/1000 as elapsed_time_ms,
                       cpu_time/1000 as cpu_time_ms, executions, buffer_gets,
                       parse_calls, first_load_time
                FROM v$sql 
                WHERE elapsed_time/1000 > :elapsed_threshold
                  AND executions > :min_executions
                ORDER BY elapsed_time DESC
                FETCH FIRST :max_results ROWS ONLY
            """
            
            thresholds = self.detection_config.get('thresholds', {})
            
            cursor.execute(sql, {
                'elapsed_threshold': thresholds.get('elapsed_time_ms', 5000),
                'min_executions': thresholds.get('executions', 10),
                'max_results': self.detection_config.get('query_settings', {}).get('max_results', 100)
            })
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'sql_id': row[0],
                    'sql_text': row[1],
                    'elapsed_time_ms': row[2],
                    'cpu_time_ms': row[3],
                    'executions': row[4],
                    'buffer_gets': row[5],
                    'parse_calls': row[6],
                    'first_load_time': row[7]
                })
            
            cursor.close()
            conn.close()
            
            self.logger.info(f"느린 SQL {len(results)}개 감지")
            return results
            
        except Exception as e:
            self.logger.error(f"느린 SQL 감지 실패: {e}")
            return []