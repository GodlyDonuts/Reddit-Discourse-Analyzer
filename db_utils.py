import sqlite3
import logging
import os
from typing import Any, List, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_NAME = "reddit_data.db"
SCHEMA_FILE = "schema.sql"

def get_connection():
    """Returns a connection to the SQLite database."""
    return sqlite3.connect(DB_NAME)

def init_db():
    """Initializes the database structure from schema.sql."""
    if not os.path.exists(SCHEMA_FILE):
        logger.error(f"Schema file {SCHEMA_FILE} not found!")
        return

    with open(SCHEMA_FILE, 'r') as f:
        schema = f.read()

    with get_connection() as conn:
        conn.executescript(schema)
        logger.info("Database initialized successfully.")

def insert_submission(submission_data: Tuple[Any, ...]):
    """Inserts a Reddit submission into the database."""
    query = """
    INSERT OR REPLACE INTO submissions (id, title, url, subreddit, created_utc)
    VALUES (?, ?, ?, ?, ?)
    """
    with get_connection() as conn:
        conn.execute(query, submission_data)
        logger.info(f"Inserted submission: {submission_data[0]}")

def insert_comments(comments_data: List[Tuple[Any, ...]]):
    """Inserts a batch of comments into the database."""
    query = """
    INSERT OR REPLACE INTO comments (comment_id, submission_id, parent_id, author, body, score, depth, permalink, created_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    with get_connection() as conn:
        conn.executemany(query, comments_data)
        logger.info(f"Inserted {len(comments_data)} comments.")

if __name__ == "__main__":
    init_db()
