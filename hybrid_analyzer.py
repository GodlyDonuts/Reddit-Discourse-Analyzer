import sqlite3
import json
import time
import os
import tenacity
from tqdm import tqdm
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

# --- Configurations ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DB_PATH = "reddit_data.db"
DB_TIMEOUT = 30 

GEMINI_MODEL = "gemini-3.1-flash-lite-preview"
ANALYZER_BATCH_SIZE = 200 
GEMINI_SLEEP_TIME = 4.3   

# --- Prompts ---

GEMINI_ANALYZER_PROMPT = """You are a high-speed data sanitizer and computational linguist for a political research project.
Your task is TWO-FOLD for each Reddit comment provided in the JSON array:

1. SANITIZE & FILTER:
   - Redact any Personally Identifiable Information (PII) like real names, phone numbers, or addresses with [REDACTED].
   - Determine if the comment should be "kept" for further analysis.
   - Keep if it contains substantive political discourse (policy, ideological debates) OR if it is a short consensus reply ("I agree", "Exactly") to a substantive parent.
   - Discard (keep=false) if it is spam, a low-effort meme, or a purely personal attack without political context.

2. CATEGORIZE (Deep Coding):
   - Determine the level of hostility: Neutral | Political_Critique | Borderline | Dehumanization.
   - Semantic Drift: Watch for moving from policy critique to essentializing ethnic groups.
   - Reasoning: One concise sentence explaining the linguistic pivot or category fit.

Return a JSON array of objects in this exact format:
[
  {
    "id": "THE_COMMENT_ID",
    "keep": true | false,
    "sanitized_text": "The redacted text",
    "category": "Neutral | Political_Critique | Borderline | Dehumanization",
    "reasoning": "Explanation of category"
  }
]
Output ONLY valid JSON."""

# --- Database Utilities ---

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT)
    conn.row_factory = sqlite3.Row
    return conn

def setup_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    # Ensure database columns and indices exist
    cols = ["category", "analysis_reasoning", "sanitized_text"]
    for col in cols:
        try:
            cursor.execute(f"ALTER TABLE comments ADD COLUMN {col} TEXT;")
        except sqlite3.OperationalError:
            pass
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_sanitized ON comments (sanitized_text);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_category ON comments (category);")
    conn.commit()
    conn.close()

def get_total_remaining():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM comments WHERE category IS NULL")
    count = cursor.fetchone()[0]
    conn.close()
    return count

def fetch_unprocessed_comments(limit=ANALYZER_BATCH_SIZE):
    conn = get_db_connection()
    cursor = conn.cursor()
    # Fetch comments where categorization results are missing
    query = """
    SELECT 
        c.comment_id, 
        c.body, 
        c.score, 
        c.parent_id,
        c.sanitized_text,
        COALESCE(p.body, s.title) as parent_text
    FROM comments c
    LEFT JOIN comments p ON c.parent_id = p.comment_id
    LEFT JOIN submissions s ON c.parent_id = s.id
    WHERE c.category IS NULL
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    rows = cursor.fetchall()
    conn.close()
    return rows

# --- Phase 2: Analysis ---

@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=60),
    reraise=True
)
def run_gemini_analyzer(client, batch):
    content = json.dumps(batch)
    
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[content],
            config=types.GenerateContentConfig(
                system_instruction=GEMINI_ANALYZER_PROMPT,
                response_mime_type='application/json',
                response_schema={
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "keep": {"type": "boolean"},
                            "sanitized_text": {"type": "string"},
                            "category": {"type": "string"},
                            "reasoning": {"type": "string"}
                        },
                        "required": ["id", "keep", "sanitized_text", "category", "reasoning"]
                    }
                }
            )
        )
        return json.loads(response.text)
    except Exception as e:
        err_str = str(e).lower()
        if "503" in err_str or "high demand" in err_str or "json" in err_str or "unterminated string" in err_str:
            tqdm.write(f"Temporary Gemini error: {e}. Retrying with backoff...")
            raise e 
        tqdm.write(f"Permanent Gemini error: {e}")
        return None

# --- Main Logic ---

def update_database(results):
    conn = get_db_connection()
    cursor = conn.cursor()
    for res in results:
        # Mark comments as skipped if the model determines they aren't substantive
        cat = res.get('category', 'Neutral') if res.get('keep') else "SKIPPED_BY_BOUNCER"
        reasoning = res.get('reasoning') or res.get('analysis_reasoning') or "No reasoning provided."
        
        cursor.execute(
            "UPDATE comments SET sanitized_text = ?, category = ?, analysis_reasoning = ? WHERE comment_id = ?",
            (res.get('sanitized_text'), cat, reasoning, res['id'])
        )
    conn.commit()
    conn.close()

def main():
    setup_db()
    
    # Initialize Clients
    if not GEMINI_API_KEY:
        print("Error: GEMINI_API_KEY environment variable not set.")
        return
        
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
    
    total_to_process = get_total_remaining()
    print(f"Starting Unified Gemini Analyzer... ({total_to_process} comments remaining)")
    print(f"Model: {GEMINI_MODEL}")
    
    pbar = tqdm(total=total_to_process, desc="Progress", unit="comment")
    
    while True:
        rows = fetch_unprocessed_comments(ANALYZER_BATCH_SIZE)
        if not rows:
            break
            
        analyzer_input = [
            {
                "id": r['comment_id'], 
                "text": r['body'],
                "score": r['score'],
                "parent_text": r['parent_text'] or "[Top Level Context]"
            } for r in rows
        ]
        
        start_time = time.time()
        results = run_gemini_analyzer(gemini_client, analyzer_input)
        
        if results:
            update_database(results)
        else:
            time.sleep(5)
            continue
            
        pbar.update(len(rows))
        
        elapsed = time.time() - start_time
        if elapsed < GEMINI_SLEEP_TIME:
            time.sleep(GEMINI_SLEEP_TIME - elapsed)
    
    pbar.close()
    print("\nProcessing complete.")

if __name__ == "__main__":
    main()
