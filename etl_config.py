"""
Configuration management for ETL Pipeline
"""

import yaml
import json
from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    port: int
    database: str
    username: str
    password: str
    driver: str = 'postgresql'
    
    def get_connection_string(self) -> str:
        """Generate SQLAlchemy connection string"""
        return f"{self.driver}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class PipelineConfig:
    """Pipeline execution configuration"""
    batch_size: int = 10000
    max_workers: int = 4
    chunk_processing: bool = True
    parallel_load: bool = False
    enable_quality_checks: bool = True
    log_level: str = 'INFO'


class ConfigManager:
    """Manage pipeline configurations"""
    
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialize configuration manager
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = Path(config_path)
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file"""
        if not self.config_path.exists():
            return self._create_default_config()
        
        with open(self.config_path, 'r') as f:
            if self.config_path.suffix == '.yaml':
                return yaml.safe_load(f)
            elif self.config_path.suffix == '.json':
                return json.load(f)
            else:
                raise ValueError(f"Unsupported config format: {self.config_path.suffix}")
    
    def _create_default_config(self) -> Dict[str, Any]:
        """Create default configuration"""
        return {
            'database': {
                'source': {
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'source_db',
                    'username': 'user',
                    'password': 'password',
                    'driver': 'postgresql'
                },
                'target': {
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'target_db',
                    'username': 'user',
                    'password': 'password',
                    'driver': 'postgresql'
                }
            },
            'pipeline': {
                'batch_size': 10000,
                'max_workers': 4,
                'chunk_processing': True,
                'parallel_load': False,
                'enable_quality_checks': True,
                'log_level': 'INFO'
            },
            'quality_checks': {
                'critical_columns': ['id'],
                'unique_keys': ['id'],
                'expected_types': {}
            }
        }
    
    def get_database_config(self, db_name: str = 'source') -> DatabaseConfig:
        """Get database configuration"""
        db_config = self.config['database'][db_name]
        return DatabaseConfig(**db_config)
    
    def get_pipeline_config(self) -> PipelineConfig:
        """Get pipeline configuration"""
        return PipelineConfig(**self.config['pipeline'])
    
    def get_quality_checks(self) -> Dict[str, Any]:
        """Get quality check configuration"""
        return self.config.get('quality_checks', {})
    
    def save_config(self, config_path: str = None) -> None:
        """Save configuration to file"""
        path = Path(config_path) if config_path else self.config_path
        
        with open(path, 'w') as f:
            if path.suffix == '.yaml':
                yaml.dump(self.config, f, default_flow_style=False)
            elif path.suffix == '.json':
                json.dump(self.config, f, indent=2)
