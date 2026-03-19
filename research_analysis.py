import sqlite3
import pandas as pd
import os

DB_PATH = "reddit_data.db"
OUTPUT_CSV = "research_analysis_results.csv"

def run_analysis():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    print("--- Connecting to Research Database ---")
    conn = sqlite3.connect(DB_PATH)
    
    # 1. Load Data into Pandas
    query = """
    SELECT 
        comment_id, 
        author, 
        score, 
        category, 
        analysis_reasoning, 
        sanitized_text,
        created_utc
    FROM comments 
    WHERE category IS NOT NULL
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        print("No analyzed data found in the database. Run hybrid_analyzer.py first.")
        return

    print(f"Loaded {len(df)} analyzed comments.\n")

    # 2. Category Distribution
    print("--- 📊 CATEGORY DISTRIBUTION ---")
    dist = df['category'].value_counts()
    percent = df['category'].value_counts(normalize=True) * 100
    summary_df = pd.DataFrame({'Count': dist, 'Percentage': percent})
    print(summary_df)
    print("\n")

    # 3. Score Analysis (Social Validation)
    print("--- 📈 SCORE ANALYSIS PER CATEGORY ---")
    score_stats = df.groupby('category')['score'].agg(['mean', 'median', 'std', 'count']).sort_values(by='mean', ascending=False)
    print(score_stats)
    print("\n")

    # 4. Top Qualitative Examples
    print("--- 📝 TOP REPRESENTATIVE EXAMPLES (By Score) ---")
    categories = df['category'].unique()
    for cat in categories:
        if cat == "SKIPPED_BY_BOUNCER":
            continue
        print(f"\n[Category: {cat}]")
        top_examples = df[df['category'] == cat].sort_values(by='score', ascending=False).head(3)
        for _, row in top_examples.iterrows():
            print(f"- Score: {row['score']} | Reasoning: {row['analysis_reasoning']}")
            print(f"  Text: {row['sanitized_text'][:150]}...")

    # 5. Export to CSV
    print(f"\n--- 💾 EXPORTING DATA ---")
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Full dataset exported to: {OUTPUT_CSV}")
    print("You can now open this file in Excel, R, or SPSS for further analysis.")

if __name__ == "__main__":
    run_analysis()
