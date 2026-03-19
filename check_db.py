import sqlite3
import logging

DB_NAME = "reddit_data.db"

def check_results():
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # Check Submissions
        cursor.execute("SELECT COUNT(*) FROM submissions")
        sub_count = cursor.fetchone()[0]
        
        # Check Comments
        cursor.execute("SELECT COUNT(*) FROM comments")
        comm_count = cursor.fetchone()[0]
        
        print("\n" + "="*30)
        print("📊 REDDIT ANALYZER STATS")
        print("="*30)
        print(f"Total Submissions Scraped: {sub_count}")
        print(f"Total Comments Stored:    {comm_count}")
        print("="*30)
        
        if comm_count > 0:
            print("\nLatest 5 Comments:")
            cursor.execute("SELECT author, score, substr(body, 1, 60) FROM comments ORDER BY created_utc DESC LIMIT 5")
            for author, score, snippet in cursor.fetchall():
                print(f"- [{author}] ({score} pts): {snippet}...")
        
        conn.close()
    except sqlite3.OperationalError:
        print(f"❌ Database '{DB_NAME}' not found. Have you run the scraper yet?")
    except Exception as e:
        print(f"❌ Error checking database: {e}")

if __name__ == "__main__":
    check_results()
