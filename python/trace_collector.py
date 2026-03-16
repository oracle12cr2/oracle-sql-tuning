#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
10046 트레이스 수집 모듈 (Phase 2)
"""

import os
import time
import paramiko
from datetime import datetime
from typing import Optional, Dict, Any
import oracledb

from utils import get_oracle_connection


class SSHClient:
    """SSH 클라이언트 (트레이스 파일 수집용)"""
    
    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger
        self.ssh_config = config['database']['ssh']
        self.client = None
    
    def connect(self):
        """SSH 연결"""
        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            self.client.connect(
                hostname=self.ssh_config['host'],
                port=self.ssh_config['port'],
                username=self.ssh_config['username'],
                key_filename=os.path.expanduser(self.ssh_config['key_file'])
            )
            
            self.logger.info("SSH 연결 성공")
            return True
            
        except Exception as e:
            self.logger.error(f"SSH 연결 실패: {e}")
            return False
    
    def execute_command(self, command: str) -> str:
        """SSH 명령 실행"""
        if not self.client:
            if not self.connect():
                return ""
        
        try:
            stdin, stdout, stderr = self.client.exec_command(command)
            output = stdout.read().decode('utf-8')
            error = stderr.read().decode('utf-8')
            
            if error:
                self.logger.warning(f"SSH 명령 오류: {error}")
            
            return output.strip()
            
        except Exception as e:
            self.logger.error(f"SSH 명령 실행 실패: {e}")
            return ""
    
    def download_file(self, remote_path: str, local_path: str) -> bool:
        """파일 다운로드"""
        if not self.client:
            if not self.connect():
                return False
        
        try:
            sftp = self.client.open_sftp()
            sftp.get(remote_path, local_path)
            sftp.close()
            
            self.logger.info(f"파일 다운로드 완료: {local_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"파일 다운로드 실패: {e}")
            return False
    
    def close(self):
        """연결 종료"""
        if self.client:
            self.client.close()


class TraceCollector:
    """10046 트레이스 수집기"""
    
    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger
        self.trace_config = config.get('trace_collection', {})
    
    def collect_trace(self, sql_id: str) -> Optional[str]:
        """10046 트레이스 수집"""
        self.logger.info(f"10046 트레이스 수집 시작: {sql_id}")
        
        try:
            conn = get_oracle_connection(self.config)
            cursor = conn.cursor()
            
            # 세션 정보 조회
            cursor.execute("SELECT SID FROM V$SESSION WHERE AUDSID = USERENV('SESSIONID')")
            sid = cursor.fetchone()[0]
            
            # 트레이스 활성화
            trace_level = self.trace_config.get('trace_level', 12)
            cursor.execute(f"ALTER SESSION SET EVENTS '10046 trace name context forever, level {trace_level}'")
            
            # SQL 실행 (샘플링)
            cursor.execute("SELECT SQL_FULLTEXT FROM V$SQL WHERE SQL_ID = :sql_id AND ROWNUM = 1", 
                          sql_id=sql_id)
            result = cursor.fetchone()
            
            if result:
                sql_text = result[0]
                # 실제로는 SQL을 실행하지 않고 EXPLAIN PLAN만 수행
                cursor.execute(f"EXPLAIN PLAN FOR {sql_text}")
                time.sleep(2)  # 트레이스 생성 대기
            
            # 트레이스 비활성화
            cursor.execute("ALTER SESSION SET EVENTS '10046 trace name context off'")
            
            cursor.close()
            conn.close()
            
            # SSH로 트레이스 파일 수집
            ssh_client = SSHClient(self.config, self.logger)
            trace_file = self._collect_trace_file(ssh_client, sid, sql_id)
            
            return trace_file
            
        except Exception as e:
            self.logger.error(f"트레이스 수집 실패: {e}")
            return None
    
    def _collect_trace_file(self, ssh_client: SSHClient, sid: int, sql_id: str) -> Optional[str]:
        """트레이스 파일 수집"""
        try:
            trace_dir = self.config['database']['trace_directory']
            
            # 최신 트레이스 파일 찾기
            find_cmd = f"find {trace_dir} -name '*{sid}*.trc' -newer /tmp/trace_start_time"
            files = ssh_client.execute_command(find_cmd)
            
            if not files:
                return None
            
            remote_file = files.split('\n')[0]
            
            # 로컬 파일 경로
            local_dir = os.path.join(self.config['output']['base_directory'], 'traces')
            os.makedirs(local_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            local_file = os.path.join(local_dir, f"trace_{sql_id}_{timestamp}.trc")
            
            if ssh_client.download_file(remote_file, local_file):
                return local_file
            
            return None
            
        except Exception as e:
            self.logger.error(f"트레이스 파일 수집 실패: {e}")
            return None