import sqlite3
import os
import pandas as pd

# Define the path to the database
db_path = os.path.join(os.path.dirname(__file__), '..', 'db', 'solarlandscape.db')

# Ensure the db directory exists
db_dir = os.path.dirname(db_path)
os.makedirs(db_dir, exist_ok=True)

# Remove existing database to recreate it
if os.path.exists(db_path):
    os.remove(db_path)

# Create the SQLite database
connection = sqlite3.connect(db_path)
cursor = connection.cursor()

# Create sample tables based on the CSV files

# Table for chart of accounts
cursor.execute('''
    CREATE TABLE IF NOT EXISTS chart_of_accounts (
        account_id INTEGER PRIMARY KEY,
        account_name TEXT NOT NULL,
        account_type TEXT,
        sgna_flag BOOLEAN,
        sgna_category TEXT
    )
''')

# Table for departments
cursor.execute('''
    CREATE TABLE IF NOT EXISTS departments (
        department_id TEXT PRIMARY KEY,
        department_name TEXT NOT NULL,
        department_group TEXT,
        headcount_planned INTEGER
    )
''')

# Table for financials monthly
cursor.execute('''
    CREATE TABLE IF NOT EXISTS financials_monthly (
        company_id INTEGER,
        company_name TEXT,
        department_id TEXT,
        account_id INTEGER,
        year_month DATE,
        actual_amount REAL,
        budget_amount REAL,
        currency TEXT,
        load_timestamp DATETIME,
        FOREIGN KEY (department_id) REFERENCES departments(department_id),
        FOREIGN KEY (account_id) REFERENCES chart_of_accounts(account_id)
    )
''')

connection.commit()

# Load data from CSVs
csv_dir = os.path.join(os.path.dirname(__file__), '..', 'csv_data')

# Load chart of accounts
chart_of_accounts_path = os.path.join(csv_dir, 'chart_of_accounts.csv')
if os.path.exists(chart_of_accounts_path):
    df_coa = pd.read_csv(chart_of_accounts_path)
    # Convert sgna_flag from 'Y'/'N' to boolean (1/0)
    df_coa['sgna_flag'] = df_coa['sgna_flag'].map({'Y': 1, 'N': 0})
    df_coa.to_sql('chart_of_accounts', connection, if_exists='append', index=False)
    print(f"Loaded {len(df_coa)} records into chart_of_accounts")

# Load departments
departments_path = os.path.join(csv_dir, 'departments.csv')
if os.path.exists(departments_path):
    df_depts = pd.read_csv(departments_path)
    df_depts.to_sql('departments', connection, if_exists='append', index=False)
    print(f"Loaded {len(df_depts)} records into departments")

# Load financials monthly
financials_path = os.path.join(csv_dir, 'financials_monthly.csv')
if os.path.exists(financials_path):
    df_financials = pd.read_csv(financials_path)
    # Convert year_month to DATE format (add day 01)
    df_financials['year_month'] = pd.to_datetime(df_financials['year_month'] + '-01').dt.date
    # Convert load_timestamp to DATETIME
    df_financials['load_timestamp'] = pd.to_datetime(df_financials['load_timestamp'])
    df_financials.to_sql('financials_monthly', connection, if_exists='append', index=False)
    print(f"Loaded {len(df_financials)} records into financials_monthly")

connection.commit()
connection.close()

print(f"Database created successfully at: {db_path}")

