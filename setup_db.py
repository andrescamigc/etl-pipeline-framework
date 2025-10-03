"""
Setup script to load sample.csv into SQLite database
Run this first to prepare your test data
"""

import pandas as pd
from sqlalchemy import create_engine

# Read your CSV file
print("Reading sample.csv...")
df = pd.read_csv('sample.csv')
print(f"Loaded {len(df)} rows from CSV")
print(f"Columns: {list(df.columns)}")
print("\nFirst few rows:")
print(df.head())

# Create SQLite database
print("\nCreating SQLite database...")
engine = create_engine('sqlite:///test_database.db')

# Save CSV data to database as 'source_table'
df.to_sql('source_table', engine, if_exists='replace', index=False)
print("✅ Data loaded into database as 'source_table'")

# Verify the data
print("\nVerifying database...")
test_df = pd.read_sql("SELECT * FROM source_table LIMIT 5", engine)
print(test_df)
print(f"\n✅ Setup complete! Database 'test_database.db' is ready.")
