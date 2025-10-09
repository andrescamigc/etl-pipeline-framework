"""
ETL Pipeline Optimization Framework
Main pipeline orchestrator with optimization features
"""

import time
import logging
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text, inspect
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
    stage_metrics: Dict[str, float] = field(default_factory=dict)
    transformations_applied: List[str] = field(default_factory=list)
    transformations_skipped: List[str] = field(default_factory=list)


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
    
    @staticmethod
    def check_value_ranges(df: pd.DataFrame, range_checks: Dict[str, tuple]) -> Dict[str, Any]:
        """Check if numeric values are within expected ranges"""
        results = {
            'passed': True,
            'issues': []
        }
        
        for col, (min_val, max_val) in range_checks.items():
            if col not in df.columns:
                continue
            
            if df[col].dtype in ['int64', 'float64']:
                out_of_range = ((df[col] < min_val) | (df[col] > max_val)).sum()
                if out_of_range > 0:
                    results['passed'] = False
                    results['issues'].append(
                        f"Column '{col}': {out_of_range} values outside range [{min_val}, {max_val}]"
                    )
        
        return results


class ETLPipeline:
    """Main ETL Pipeline with optimization features and best practices"""
    
    def __init__(self, 
                 db_connection_string: str, 
                 batch_size: int = 10000, 
                 interactive: bool = True,
                 log_level: str = 'INFO'):
        """
        Initialize ETL Pipeline
        
        Args:
            db_connection_string: SQLAlchemy connection string
            batch_size: Number of rows to process per batch
            interactive: Whether to show previews and ask for confirmation
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.engine = create_engine(db_connection_string, pool_pre_ping=True)
        self.batch_size = batch_size
        self.interactive = interactive
        self.quality_checker = DataQualityChecker()
        self.metrics = PipelineMetrics(start_time=datetime.now())
        
        # Set logging level
        logger.setLevel(getattr(logging, log_level.upper()))
        
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table in the database"""
        inspector = inspect(self.engine)
        
        if not inspector.has_table(table_name):
            return {'exists': False}
        
        columns = inspector.get_columns(table_name)
        row_count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name}", self.engine).iloc[0]['count']
        
        return {
            'exists': True,
            'columns': [col['name'] for col in columns],
            'column_types': {col['name']: str(col['type']) for col in columns},
            'row_count': row_count
        }
        
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
            logger.debug(f"Columns: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _show_transformation_preview(self, 
                                     df_before: pd.DataFrame, 
                                     df_after: pd.DataFrame, 
                                     transform_name: str) -> bool:
        """
        Show preview of transformation changes and ask for confirmation
        
        Returns:
            True if user accepts, False if user rejects
        """
        print("\n" + "=" * 70)
        print(f"TRANSFORMATION PREVIEW: {transform_name}")
        print("=" * 70)
        
        # Show sample rows (first 5)
        sample_size = min(5, len(df_before))
        
        print(f"\n📊 Showing {sample_size} sample rows")
        print("\nBEFORE:")
        print("-" * 70)
        print(df_before.head(sample_size).to_string())
        
        print("\n\nAFTER:")
        print("-" * 70)
        print(df_after.head(sample_size).to_string())
        
        # Show column changes
        print("\n\n📋 COLUMN CHANGES:")
        print("-" * 70)
        
        cols_before = set(df_before.columns)
        cols_after = set(df_after.columns)
        
        new_cols = cols_after - cols_before
        removed_cols = cols_before - cols_after
        
        if new_cols:
            print(f"✅ New columns added: {list(new_cols)}")
        if removed_cols:
            print(f"❌ Columns removed: {list(removed_cols)}")
        if not new_cols and not removed_cols:
            print("ℹ️  No columns added or removed")
        
        # Show data type changes
        common_cols = cols_before & cols_after
        type_changes = []
        for col in common_cols:
            if df_before[col].dtype != df_after[col].dtype:
                type_changes.append(
                    f"  • {col}: {df_before[col].dtype} → {df_after[col].dtype}"
                )
        
        if type_changes:
            print("\n🔄 Data type changes:")
            for change in type_changes:
                print(change)
        
        # Show value changes for text columns
        print("\n\n📝 SAMPLE VALUE CHANGES:")
        print("-" * 70)
        text_cols = df_before.select_dtypes(include=['object']).columns
        changes_shown = 0
        
        for col in common_cols:
            if col in text_cols and changes_shown < 3:
                # Find rows where values changed
                for idx in df_before.index[:sample_size]:
                    if idx in df_after.index:
                        val_before = df_before.loc[idx, col]
                        val_after = df_after.loc[idx, col]
                        if val_before != val_after and pd.notna(val_before) and pd.notna(val_after):
                            print(f"\nColumn '{col}', Row {idx}:")
                            print(f"  Before: '{val_before}'")
                            print(f"  After:  '{val_after}'")
                            changes_shown += 1
                            if changes_shown >= 3:
                                break
        
        if changes_shown == 0:
            print("ℹ️  No text value changes detected in sample")
        
        # Show row count change
        if len(df_before) != len(df_after):
            print(f"\n⚠️  Row count changed: {len(df_before)} → {len(df_after)}")
        
        # Ask for confirmation
        print("\n" + "=" * 70)
        while True:
            response = input("Do you want to apply this transformation? (yes/no): ").strip().lower()
            if response in ['yes', 'y']:
                print("✅ Transformation accepted")
                return True
            elif response in ['no', 'n']:
                print("❌ Transformation rejected - skipping")
                return False
            else:
                print("Please enter 'yes' or 'no'")
    
    def transform(self, 
                  df: pd.DataFrame, 
                  transformations: List[Callable[[pd.DataFrame], pd.DataFrame]],
                  quality_checks: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Transform data with quality checks and interactive preview
        
        Args:
            df: Input DataFrame
            transformations: List of transformation functions
            quality_checks: Dictionary with quality check parameters
        """
        logger.info("Starting transformation phase...")
        start_time = time.time()
        
        try:
            # Apply transformations with preview
            for transform_fn in transformations:
                logger.info(f"Processing transformation: {transform_fn.__name__}")
                
                df_before = df.copy()
                
                try:
                    df_after = transform_fn(df)
                except Exception as e:
                    logger.error(f"Transformation '{transform_fn.__name__}' failed: {str(e)}")
                    raise
                
                # Show preview and get confirmation if in interactive mode
                if self.interactive:
                    accepted = self._show_transformation_preview(
                        df_before, df_after, transform_fn.__name__
                    )
                    if accepted:
                        df = df_after
                        self.metrics.transformations_applied.append(transform_fn.__name__)
                        logger.info(f"✅ Applied transformation: {transform_fn.__name__}")
                    else:
                        self.metrics.transformations_skipped.append(transform_fn.__name__)
                        logger.info(f"⏭️  Skipped transformation: {transform_fn.__name__}")
                else:
                    df = df_after
                    self.metrics.transformations_applied.append(transform_fn.__name__)
                    logger.info(f"✅ Applied transformation: {transform_fn.__name__}")
            
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
    
    def load(self, 
             df: pd.DataFrame, 
             table_name: str, 
             if_exists: str = 'append', 
             parallel: bool = False,
             create_index: Optional[List[str]] = None) -> None:
        """
        Load data to destination with optional parallel processing
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            parallel: Whether to use parallel batch loading
            create_index: List of columns to create indexes on
        """
        logger.info(f"Starting load phase to table '{table_name}'...")
        start_time = time.time()
        
        try:
            if parallel and len(df) > self.batch_size:
                self._parallel_load(df, table_name, if_exists)
            else:
                df.to_sql(table_name, self.engine, if_exists=if_exists, 
                         index=False, chunksize=self.batch_size)
            
            # Create indexes if specified
            if create_index:
                self._create_indexes(table_name, create_index)
            
            elapsed = time.time() - start_time
            self.metrics.stage_metrics['load'] = elapsed
            self.metrics.rows_processed = len(df)
            logger.info(f"Loaded {len(df)} rows in {elapsed:.2f}s")
            
        except Exception as e:
            logger.error(f"Load failed: {str(e)}")
            raise
    
    def _parallel_load(self, df: pd.DataFrame, table_name: str, if_exists: str) -> None:
        """Load data in parallel batches"""
        logger.info(f"Using parallel loading with {4} workers")
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
    
    def _create_indexes(self, table_name: str, columns: List[str]) -> None:
        """Create indexes on specified columns"""
        logger.info(f"Creating indexes on columns: {columns}")
        with self.engine.connect() as conn:
            for col in columns:
                index_name = f"idx_{table_name}_{col}"
                try:
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({col})"))
                    conn.commit()
                    logger.info(f"Created index: {index_name}")
                except Exception as e:
                    logger.warning(f"Could not create index on {col}: {str(e)}")
    
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
        
        if 'value_ranges' in checks:
            result = self.quality_checker.check_value_ranges(
                df, checks['value_ranges']
            )
            if not result['passed']:
                logger.warning(f"Range check issues: {result['issues']}")
        
        logger.info("✅ All quality checks passed")
    
    def run(self, 
            extract_query: str, 
            transformations: List[Callable[[pd.DataFrame], pd.DataFrame]],
            load_table: str, 
            quality_checks: Optional[Dict[str, Any]] = None,
            **kwargs) -> PipelineMetrics:
        """
        Execute complete ETL pipeline
        
        Args:
            extract_query: SQL query for extraction
            transformations: List of transformation functions
            load_table: Target table name
            quality_checks: Quality check parameters
            **kwargs: Additional parameters
                - if_exists: 'fail', 'replace', or 'append' (default: 'append')
                - parallel: Use parallel loading (default: False)
                - chunk_processing: Use chunked extraction (default: True)
                - create_index: List of columns to index (default: None)
        
        Returns:
            PipelineMetrics object with execution statistics
        """
        logger.info("=" * 70)
        logger.info("STARTING ETL PIPELINE EXECUTION")
        logger.info("=" * 70)
        
        try:
            # Extract
            df = self.extract(
                extract_query, 
                chunk_processing=kwargs.get('chunk_processing', True)
            )
            
            # Transform
            df = self.transform(df, transformations, quality_checks)
            
            # Load
            self.load(
                df, 
                load_table, 
                if_exists=kwargs.get('if_exists', 'append'),
                parallel=kwargs.get('parallel', False),
                create_index=kwargs.get('create_index', None)
            )
            
            # Finalize metrics
            self.metrics.end_time = datetime.now()
            self.metrics.processing_time = sum(self.metrics.stage_metrics.values())
            
            logger.info("=" * 70)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 70)
            logger.info(f"📊 Total rows processed: {self.metrics.rows_processed:,}")
            logger.info(f"⏱️  Total processing time: {self.metrics.processing_time:.2f}s")
            logger.info(f"✅ Transformations applied: {len(self.metrics.transformations_applied)}")
            if self.metrics.transformations_skipped:
                logger.info(f"⏭️  Transformations skipped: {len(self.metrics.transformations_skipped)}")
            logger.info("\nStage breakdown:")
            for stage, time_taken in self.metrics.stage_metrics.items():
                logger.info(f"  • {stage.capitalize()}: {time_taken:.2f}s")
            logger.info("=" * 70)
            
            return self.metrics
            
        except Exception as e:
            logger.error("=" * 70)
            logger.error("PIPELINE FAILED")
            logger.error("=" * 70)
            logger.error(f"Error: {str(e)}")
            raise
    
    def validate_data(self, table_name: str, validation_query: str) -> pd.DataFrame:
        """
        Run a validation query against the loaded data
        
        Args:
            table_name: Table to validate
            validation_query: SQL query for validation
            
        Returns:
            DataFrame with validation results
        """
        logger.info(f"Running validation on table '{table_name}'...")
        try:
            result = pd.read_sql(validation_query, self.engine)
            logger.info("Validation completed")
            return result
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            raise


if __name__ == "__main__":
    # This file should not be run directly - it's a library
    # Import and use from your run_pipeline.py script
    print("This is the ETL Pipeline framework library.")
    print("Import it in your run_pipeline.py script to use it.")
    print("\nExample:")
    print("  from etl_pipeline import ETLPipeline")
    print("  pipeline = ETLPipeline('sqlite:///mydb.db')")