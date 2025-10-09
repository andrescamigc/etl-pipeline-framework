"""
Setup script to upload CSV file and load it into SQLite database
Run this to prepare your test data from any CSV file
"""
import pandas as pd
from sqlalchemy import create_engine
from tkinter import Tk, filedialog

# Hide the main tkinter window
root = Tk()
root.withdraw()
root.attributes('-topmost', True)

# Prompt user to select a CSV file
print("Please select your CSV file...")
csv_path = filedialog.askopenfilename(
    title="Select CSV File",
    filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
)

# Check if user cancelled the dialog
if not csv_path:
    print("❌ No file selected. Exiting.")
    exit()

# Read the selected CSV file
print(f"\nReading {csv_path}...")
try:
    df = pd.read_csv(csv_path)
    print(f"✅ Loaded {len(df)} rows from CSV")
    print(f"Columns: {list(df.columns)}")
    print("\nFirst few rows:")
    print(df.head())
except Exception as e:
    print(f"❌ Error reading CSV: {e}")
    exit()

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