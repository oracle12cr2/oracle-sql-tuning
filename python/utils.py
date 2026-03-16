#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
공통 유틸리티 모듈
"""

import os
import yaml
import logging
from typing import Dict, Any
import oracledb


def load_config(config_path: str = None) -> Dict[str, Any]:
    """설정 파일 로드"""
    if config_path is None:
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'settings.yaml')
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    return config


def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    """로깅 설정"""
    log_config = config.get('logging', {})
    
    # 로그 디렉터리 생성
    log_file = log_config.get('file', 'logs/sql_tuning.log')
    log_dir = os.path.dirname(log_file)
    os.makedirs(log_dir, exist_ok=True)
    
    # 로깅 설정
    logging.basicConfig(
        level=getattr(logging, log_config.get('level', 'INFO')),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger('sql_tuning')
    return logger


def ensure_directories(config: Dict[str, Any]):
    """필요한 디렉터리 생성"""
    base_dir = config['output']['base_directory']
    
    for dir_name in config['output']['directories'].values():
        dir_path = os.path.join(base_dir, dir_name)
        os.makedirs(dir_path, exist_ok=True)


def get_oracle_connection(config: Dict[str, Any]):
    """Oracle DB 연결"""
    db_config = config['database']
    
    dsn = f"{db_config['host']}:{db_config['port']}/{db_config['service_name']}"
    
    connection = oracledb.connect(
        user=db_config['username'],
        password=db_config['password'],
        dsn=dsn
    )
    
    return connection