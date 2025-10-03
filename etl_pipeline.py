"""
ETL Pipeline Optimization Framework
Main pipeline orchestrator with optimization features
"""

import time
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class PipelineMetrics:
    """Store pipeline execution metrics"""
    start_time: datetime
    end_time: Optional[datetime] = None
    rows_processed: int = 0
    rows_failed: int = 0
    processing_time: float = 0.0
    stage_metrics: Dict[str, float] = None
    
    def __post_init__(self):
        if self.stage_metrics is None:
            self.stage_metrics = {}


class DataQualityChecker:
    """Perform data quality validations"""
    
    @staticmethod
    def check_null_values(df: pd.DataFrame, critical_columns: List[str]) -> Dict[str, Any]:
        """Check for null values in critical columns"""
        results = {
            'passed': True,
            'issues': []
        }
        
        for col in critical_columns:
            if col not in df.columns:
                results['passed'] = False
                results['issues'].append(f"Missing column: {col}")
                continue
                
            null_count = df[col].isnull().sum()
            if null_count > 0:
                results['passed'] = False
                results['issues'].append(
                    f"Column '{col}' has {null_count} null values"
                )
        
        return results
    
    @staticmethod
    def check_duplicates(df: pd.DataFrame, key_columns: List[str]) -> Dict[str, Any]:
        """Check for duplicate records"""
        duplicate_count = df.duplicated(subset=key_columns).sum()
        
        return {
            'passed': duplicate_count == 0,
            'duplicate_count': int(duplicate_count),
            'message': f"Found {duplicate_count} duplicate records"
        }
    
    @staticmethod
    def check_data_types(df: pd.DataFrame, expected_types: Dict[str, str]) -> Dict[str, Any]:
        """Validate data types"""
        results = {
            'passed': True,
            'issues': []
        }
        
        for col, expected_type in expected_types.items():
            if col not in df.columns:
                continue
            
            actual_type = str(df[col].dtype)
            if not actual_type.startswith(expected_type):
                results['passed'] = False
                results['issues'].append(
                    f"Column '{col}': expected {expected_type}, got {actual_type}"
                )
        
        return results


class ETLPipeline:
    """Main ETL Pipeline with optimization features"""
    
    def __init__(self, db_connection_string: str, batch_size: int = 10000):
        """
        Initialize ETL Pipeline
        
        Args:
            db_connection_string: SQLAlchemy connection string
            batch_size: Number of rows to process per batch
        """
        self.engine = create_engine(db_connection_string)
        self.batch_size = batch_size
        self.quality_checker = DataQualityChecker()
        self.metrics = PipelineMetrics(start_time=datetime.now())
        
    def extract(self, query: str, chunk_processing: bool = True) -> pd.DataFrame:
        """
        Extract data from source with optional chunked reading
        
        Args:
            query: SQL query to extract data
            chunk_processing: Whether to use chunked reading for large datasets
        """
        logger.info("Starting extraction phase...")
        start_time = time.time()
        
        try:
            if chunk_processing:
                # Read in chunks for memory efficiency
                chunks = []
                for chunk in pd.read_sql(query, self.engine, chunksize=self.batch_size):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
            else:
                df = pd.read_sql(query, self.engine)
            
            elapsed = time.time() - start_time
            self.metrics.stage_metrics['extract'] = elapsed
            logger.info(f"Extracted {len(df)} rows in {elapsed:.2f}s")
            
            return df
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def transform(self, df: pd.DataFrame, 
                  transformations: List[callable],
                  quality_checks: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Transform data with quality checks
        
        Args:
            df: Input DataFrame
            transformations: List of transformation functions
            quality_checks: Dictionary with quality check parameters
        """
        logger.info("Starting transformation phase...")
        start_time = time.time()
        
        try:
            # Apply transformations
            for transform_fn in transformations:
                df = transform_fn(df)
                logger.info(f"Applied transformation: {transform_fn.__name__}")
            
            # Run quality checks if specified
            if quality_checks:
                self._run_quality_checks(df, quality_checks)
            
            elapsed = time.time() - start_time
            self.metrics.stage_metrics['transform'] = elapsed
            logger.info(f"Transformed {len(df)} rows in {elapsed:.2f}s")
            
            return df
            
        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def load(self, df: pd.DataFrame, table_name: str, 
             if_exists: str = 'append', parallel: bool = False) -> None:
        """
        Load data to destination with optional parallel processing
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            parallel: Whether to use parallel batch loading
        """
        logger.info(f"Starting load phase to table '{table_name}'...")
        start_time = time.time()
        
        try:
            if parallel and len(df) > self.batch_size:
                self._parallel_load(df, table_name, if_exists)
            else:
                df.to_sql(table_name, self.engine, if_exists=if_exists, 
                         index=False, chunksize=self.batch_size)
            
            elapsed = time.time() - start_time
            self.metrics.stage_metrics['load'] = elapsed
            self.metrics.rows_processed = len(df)
            logger.info(f"Loaded {len(df)} rows in {elapsed:.2f}s")
            
        except Exception as e:
            logger.error(f"Load failed: {str(e)}")
            raise
    
    def _parallel_load(self, df: pd.DataFrame, table_name: str, if_exists: str) -> None:
        """Load data in parallel batches"""
        batches = [df[i:i + self.batch_size] for i in range(0, len(df), self.batch_size)]
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for i, batch in enumerate(batches):
                mode = if_exists if i == 0 else 'append'
                future = executor.submit(
                    batch.to_sql, table_name, self.engine, 
                    if_exists=mode, index=False
                )
                futures.append(future)
            
            for future in as_completed(futures):
                future.result()  # Raise exception if any occurred
    
    def _run_quality_checks(self, df: pd.DataFrame, checks: Dict[str, Any]) -> None:
        """Run data quality checks"""
        logger.info("Running quality checks...")
        
        if 'critical_columns' in checks:
            result = self.quality_checker.check_null_values(
                df, checks['critical_columns']
            )
            if not result['passed']:
                raise ValueError(f"Quality check failed: {result['issues']}")
        
        if 'unique_keys' in checks:
            result = self.quality_checker.check_duplicates(
                df, checks['unique_keys']
            )
            if not result['passed']:
                logger.warning(result['message'])
        
        if 'expected_types' in checks:
            result = self.quality_checker.check_data_types(
                df, checks['expected_types']
            )
            if not result['passed']:
                raise ValueError(f"Type check failed: {result['issues']}")
        
        logger.info("All quality checks passed")
    
    def run(self, extract_query: str, transformations: List[callable],
            load_table: str, quality_checks: Optional[Dict[str, Any]] = None,
            **kwargs) -> PipelineMetrics:
        """
        Execute complete ETL pipeline
        
        Args:
            extract_query: SQL query for extraction
            transformations: List of transformation functions
            load_table: Target table name
            quality_checks: Quality check parameters
            **kwargs: Additional parameters (if_exists, parallel, etc.)
        
        Returns:
            PipelineMetrics object with execution statistics
        """
        logger.info("=" * 50)
        logger.info("Starting ETL Pipeline Execution")
        logger.info("=" * 50)
        
        try:
            # Extract
            df = self.extract(extract_query, 
                            chunk_processing=kwargs.get('chunk_processing', True))
            
            # Transform
            df = self.transform(df, transformations, quality_checks)
            
            # Load
            self.load(df, load_table, 
                     if_exists=kwargs.get('if_exists', 'append'),
                     parallel=kwargs.get('parallel', False))
            
            # Finalize metrics
            self.metrics.end_time = datetime.now()
            self.metrics.processing_time = sum(self.metrics.stage_metrics.values())
            
            logger.info("=" * 50)
            logger.info("Pipeline Completed Successfully")
            logger.info(f"Total time: {self.metrics.processing_time:.2f}s")
            logger.info(f"Rows processed: {self.metrics.rows_processed}")
            logger.info("=" * 50)
            
            return self.metrics
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise


# Example transformation functions
def clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Clean text columns by stripping whitespace"""
    text_cols = df.select_dtypes(include=['object']).columns
    for col in text_cols:
        df[col] = df[col].str.strip()
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handle missing values with appropriate strategies"""
    # Example: forward fill for time-series data
    numeric_cols = df.select_dtypes(include=['number']).columns
    df[numeric_cols] = df[numeric_cols].fillna(method='ffill')
    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns for analysis"""
    # Example: add timestamp for audit
    df['processed_at'] = datetime.now()
    return df


if __name__ == "__main__":
    # Example usage
    CONNECTION_STRING = "sqlite:///example.db"
    
    pipeline = ETLPipeline(CONNECTION_STRING, batch_size=5000)
    
    # Define transformations
    transformations = [
        clean_text_columns,
        handle_missing_values,
        add_derived_columns
    ]
    
    # Define quality checks
    quality_checks = {
        'critical_columns': ['id', 'name'],
        'unique_keys': ['id'],
        'expected_types': {
            'id': 'int',
            'name': 'object'
        }
    }
    
    # Run pipeline
    metrics = pipeline.run(
        extract_query="SELECT * FROM source_table",
        transformations=transformations,
        load_table="target_table",
        quality_checks=quality_checks,
        if_exists='replace',
        parallel=True
    )