# Complete Setup Guide

This guide will walk you through setting up the ETL Pipeline Optimization Framework from scratch.

## 📋 Table of Contents

1. [System Requirements](#system-requirements)
2. [Installation Steps](#installation-steps)
3. [Database Setup](#database-setup)
4. [Configuration](#configuration)
5. [Running Your First Pipeline](#running-your-first-pipeline)
6. [Troubleshooting](#troubleshooting)

---

## 🖥️ System Requirements

### Minimum Requirements
- **Python**: 3.8 or higher
- **RAM**: 4GB minimum (8GB recommended)
- **Storage**: 500MB for dependencies + your data
- **OS**: Windows 10+, macOS 10.14+, or Linux (Ubuntu 18.04+)

### Required Software
- Python 3.8+ ([Download](https://www.python.org/downloads/))
- pip (usually comes with Python)
- Git ([Download](https://git-scm.com/downloads))
- Database (choose one):
  - SQLite (built-in, good for testing)
  - PostgreSQL ([Download](https://www.postgresql.org/download/))
  - MySQL ([Download](https://dev.mysql.com/downloads/))

---

## 🚀 Installation Steps

### Step 1: Verify Python Installation

Open terminal/command prompt and run:

```bash
python --version
# Should show: Python 3.8.x or higher

pip --version
# Should show pip version
```

**If Python is not installed:**
- Windows: Download from [python.org](https://www.python.org/downloads/)
- Mac: `brew install python3`
- Linux: `sudo apt-get install python3 python3-pip`

### Step 2: Clone the Repository

```bash
# Clone the repository
git clone https://github.com/andrescamigc/etl-pipeline-framework.git

# Navigate to the project directory
cd etl-pipeline-framework

# Verify you're in the right place
ls
# You should see: README.md, requirements.txt, etl_pipeline.py, etc.
```

### Step 3: Create Virtual Environment (Recommended)

**Why?** Keeps project dependencies isolated from your system Python.

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**Mac/Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

You should see `(venv)` at the start of your command prompt.

### Step 4: Install Dependencies

```bash
pip install -r requirements.txt
```

This installs:
- pandas (data manipulation)
- SQLAlchemy (database connectivity)
- PyYAML (configuration files)
- psycopg2-binary (PostgreSQL driver)
- pymysql (MySQL driver)
- pytest (testing framework)

**Verify installation:**
```bash
python -c "import pandas; import sqlalchemy; print('✅ All packages installed!')"
```

---

## 🗄️ Database Setup

### Option 1: SQLite (Easiest - Recommended for Testing)

No setup needed! SQLite creates a file-based database automatically.

```bash
# Generate sample data
python create_sample_csv.py

# Setup database
python setup_db.py
```

This creates `test_database.db` in your project folder.

### Option 2: PostgreSQL (Recommended for Production)

**Install PostgreSQL:**
- Download from [postgresql.org](https://www.postgresql.org/download/)
- Follow installation wizard
- Remember your password!

**Create Database:**
```sql
-- Open psql terminal or pgAdmin
CREATE DATABASE etl_source_db;
CREATE DATABASE etl_target_db;

-- Create user (optional)
CREATE USER etl_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE etl_source_db TO etl_user;
GRANT ALL PRIVILEGES ON DATABASE etl_target_db TO etl_user;
```

**Update configuration:**
```yaml
# config.yaml
database:
  source:
    host: localhost
    port: 5432
    database: etl_source_db
    username: etl_user
    password: your_password
    driver: postgresql
```

### Option 3: MySQL

**Install MySQL:**
- Download from [mysql.com](https://dev.mysql.com/downloads/)
- Follow installation wizard

**Create Database:**
```sql
-- Open MySQL Workbench or terminal
CREATE DATABASE etl_source_db;
CREATE DATABASE etl_target_db;

-- Create user
CREATE USER 'etl_user'@'localhost' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON etl_source_db.* TO 'etl_user'@'localhost';
GRANT ALL PRIVILEGES ON etl_target_db.* TO 'etl_user'@'localhost';
FLUSH PRIVILEGES;
```

**Update configuration:**
```yaml
# config.yaml
database:
  source:
    host: localhost
    port: 3306
    database: etl_source_db
    username: etl_user
    password: your_password
    driver: mysql+pymysql
```

---

## ⚙️ Configuration

### Step 1: Create Configuration File

```bash
# Copy example configuration
cp config.example.yaml config.yaml

# Or on Windows:
copy config.example.yaml config.yaml
```

### Step 2: Edit Configuration

Open `config.yaml` in your text editor:

```yaml
database:
  source:
    host: localhost          # Change if database is remote
    port: 5432              # 5432 for PostgreSQL, 3306 for MySQL
    database: your_db_name   # Your database name
    username: your_username  # Your username
    password: your_password  # Your password
    driver: postgresql       # postgresql, mysql+pymysql, or sqlite

pipeline:
  batch_size: 10000         # Adjust based on your RAM
  max_workers: 4            # Adjust based on your CPU cores
  chunk_processing: true    # Keep true for large datasets
  parallel_load: false      # Set true for faster loading
  enable_quality_checks: true
  log_level: INFO           # DEBUG for more details

quality_checks:
  critical_columns:
    - customer_id           # Columns that can't be null
    - email
  unique_keys:
    - customer_id           # Columns that must be unique
  expected_types:
    customer_id: int        # Expected data types
    email: object
```

### Step 3: Secure Your Credentials (Optional but Recommended)

**Using Environment Variables:**

Create `.env` file:
```bash
cp .env.example .env
```

Edit `.env`:
```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mydb
DB_USER=user
DB_PASSWORD=secret_password
```

**Load in your code:**
```python
from dotenv import load_dotenv
import os

load_dotenv()
password = os.getenv('DB_PASSWORD')
```

**Important:** `.env` is in `.gitignore` - won't be committed to GitHub!

---

## 🎯 Running Your First Pipeline

### Quick Test with Sample Data

```bash
# 1. Generate sample CSV data
python create_sample_csv.py

# 2. Load CSV into database
python setup_db.py

# 3. Run the pipeline
python run_pipeline.py
```

**Expected Output:**
```
============================================================
ETL PIPELINE TEST RUN
============================================================

1. Initializing pipeline...
✅ Pipeline initialized

2. Defining transformations...
✅ Transformations defined

3. Setting up quality checks...
✅ Quality checks configured

4. Running ETL Pipeline...
------------------------------------------------------------
2025-10-03 10:30:00 - INFO - Starting extraction phase...
2025-10-03 10:30:02 - INFO - Extracted 50 rows in 0.15s
...
============================================================
✅ Status: SUCCESS
📊 Rows Processed: 50
⏱️  Total Time: 0.56 seconds
```

### Running with Your Own Data

**Step 1: Prepare your data**
```bash
# If you have a CSV file
cp /path/to/your/data.csv sample.csv

# Or if data is already in database, skip to Step 2
```

**Step 2: Customize the pipeline**

Edit `run_pipeline.py`:

```python
# Update the extract query for your table
extract_query = "SELECT * FROM your_table_name WHERE date > '2024-01-01'"

# Update quality checks for your columns
quality_checks = {
    'critical_columns': ['your_id_column', 'your_critical_column'],
    'unique_keys': ['your_id_column']
}

# Customize transformations
def your_custom_transformation(df):
    # Your logic here
    df['new_column'] = df['old_column'] * 2
    return df

transformations = [
    clean_text_data,
    your_custom_transformation,
    add_processing_metadata
]
```

**Step 3: Run it**
```bash
python run_pipeline.py
```

---

## 📚 Additional Resources

- [README.md](README.md) - Project overview and features
- [CONTRIBUTING.md](CONTRIBUTING.md) - How to contribute
- [API Documentation](#) - Detailed API reference
- [Best Practices](#) - ETL optimization tips
