"""
Simple script to run the ETL pipeline with your sample data
"""

import os
import pandas as pd
from datetime import datetime
from tkinter import Tk, filedialog
from etl_pipeline import ETLPipeline

# ===== DATABASE SELECTION =====
# Hide the main tkinter window
root = Tk()
root.withdraw()
root.attributes('-topmost', True)

# Set initial directory to databases folder if it exists
initial_dir = 'databases' if os.path.exists('databases') else '.'

# Prompt user to select a database file
print("Please select your database file...")
db_path = filedialog.askopenfilename(
    title="Select Database File",
    initialdir=initial_dir,
    filetypes=[("SQLite Database", "*.db"), ("All files", "*.*")]
)

# Check if user cancelled the dialog
if not db_path:
    print("❌ No database selected. Exiting.")
    exit()

DATABASE_FILE = db_path
print(f"✅ Selected database: {DATABASE_FILE}")

# ===== CONFIGURATION =====
CONNECTION_STRING = f'sqlite:///{DATABASE_FILE}'
SOURCE_TABLE = 'source_table'
TARGET_TABLE = 'processed_data'
BATCH_SIZE = 1000

print("=" * 60)
print("ETL PIPELINE TEST RUN")
print("=" * 60)

# ===== STEP 1: Initialize Pipeline =====
print("\n1. Initializing pipeline...")
pipeline = ETLPipeline(
    db_connection_string=CONNECTION_STRING,
    batch_size=BATCH_SIZE
)
print("✅ Pipeline initialized")

# ===== STEP 2: Define Custom Transformations =====
print("\n2. Defining transformations...")

def clean_text_data(df):
    """Remove extra whitespace from text columns"""
    print("   - Cleaning text columns...")
    text_cols = df.select_dtypes(include=['object']).columns
    for col in text_cols:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.strip()
    return df

def add_processing_metadata(df):
    """Add metadata columns"""
    print("   - Adding metadata columns...")
    df['processed_at'] = datetime.now()
    df['processing_batch'] = 1
    return df

def handle_missing_data(df):
    """Handle missing values"""
    print("   - Handling missing values...")
    # Fill numeric columns with 0
    numeric_cols = df.select_dtypes(include=['number']).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    # Fill text columns with 'Unknown'
    text_cols = df.select_dtypes(include=['object']).columns
    df[text_cols] = df[text_cols].fillna('Unknown')
    
    return df

transformations = [
    clean_text_data,
    handle_missing_data,
    add_processing_metadata
]
print("✅ Transformations defined")

# ===== STEP 3: Define Quality Checks =====
print("\n3. Setting up quality checks...")

# Get first column name as ID column (customize this based on your CSV)
sample_df = pd.read_sql(f"SELECT * FROM {SOURCE_TABLE} LIMIT 1", pipeline.engine)
columns = list(sample_df.columns)
print(f"   Available columns: {columns}")

# Basic quality checks (customize based on your data)
quality_checks = {
    'critical_columns': [columns[0]],  # First column as critical
    'unique_keys': [columns[0]]  # First column should be unique
}
print(f"✅ Quality checks configured for column: {columns[0]}")

# ===== STEP 4: Run the Pipeline =====
print("\n4. Running ETL Pipeline...")
print("-" * 60)

try:
    metrics = pipeline.run(
        extract_query=f"SELECT * FROM {SOURCE_TABLE}",
        transformations=transformations,
        load_table=TARGET_TABLE,
        quality_checks=quality_checks,
        if_exists='replace',  # Replace table if exists
        chunk_processing=True,
        parallel=False  # Set to True for larger datasets
    )
    
    # ===== STEP 5: Show Results =====
    print("\n" + "=" * 60)
    print("PIPELINE EXECUTION SUMMARY")
    print("=" * 60)
    print(f"✅ Status: SUCCESS")
    print(f"📊 Rows Processed: {metrics.rows_processed:,}")
    print(f"⏱️  Total Time: {metrics.processing_time:.2f} seconds")
    print(f"\nStage Breakdown:")
    for stage, time in metrics.stage_metrics.items():
        print(f"  - {stage.capitalize()}: {time:.2f}s")
    
    # ===== STEP 6: Verify Results =====
    print("\n" + "=" * 60)
    print("VERIFYING PROCESSED DATA")
    print("=" * 60)
    
    result_df = pd.read_sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 10", pipeline.engine)
    print(f"\nFirst 10 rows of processed data:")
    print(result_df)
    
    print(f"\n✅ Pipeline completed successfully!")
    print(f"📁 Output saved to table: '{TARGET_TABLE}' in '{DATABASE_FILE}'")
    
except Exception as e:
    print("\n" + "=" * 60)
    print("❌ PIPELINE FAILED")
    print("=" * 60)
    print(f"Error: {str(e)}")
    print("\nTroubleshooting tips:")
    print("1. Make sure you selected the correct database file")
    print("2. Run 'setup_database.py' first to create the database from your CSV")
    print("3. Check that all required columns exist in your data")
    raise

print("\n" + "=" * 60)
print("To view your data, you can:")
print(f"1. Run: python -c \"import pandas as pd; print(pd.read_sql('SELECT * FROM {TARGET_TABLE}', 'sqlite:///{DATABASE_FILE}'))\"")
print(f"2. Or use a SQLite browser to open '{DATABASE_FILE}'")
print("=" * 60)