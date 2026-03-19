import sqlite3
import pandas as pd
import os

DB_PATH = "reddit_data.db"

def export_all_to_csv():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    
    print(f"--- 💾 Starting Excel-Friendly Export (Total Tables: {len(tables)}) ---")
    
    for table in tables:
        print(f"Processing table: {table}...")
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        
        # Human-readable date conversions
        if 'created_utc' in df.columns:
            df['created_at_utc'] = pd.to_datetime(df['created_utc'], unit='s')
            df['human_readable_date'] = df['created_at_utc'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        output_file = f"excel_export_{table}.csv"
        df.to_csv(output_file, index=False)
        print(f"✅ Exported: {output_file}")

    conn.close()
    print("\n--- ✨ All tables exported successfully! ---")
    print("You can now open these 'excel_export_*.csv' files directly in Excel.")

if __name__ == "__main__":
    export_all_to_csv()
