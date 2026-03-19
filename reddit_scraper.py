import requests
import logging
import time
import os
import urllib.parse
import datetime
import random
import sys
import argparse
from typing import List, Tuple, Any, Dict, Optional
from db_utils import init_db, insert_submission, insert_comments

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 RedditAnalyzer/1.0 (Research Pipeline for Computer Science)"

def fetch_json(url: str) -> Optional[List[Dict[str, Any]]]:
    """Fetches JSON data from a Reddit URL, ensuring .json is correctly placed."""
    parsed = urllib.parse.urlparse(url)
    path = parsed.path
    if not path.endswith(".json"):
        path = path.rstrip('/') + ".json"
    
    # Reconstruct URL with the correct path and original query params
    url = urllib.parse.urlunparse((
        parsed.scheme,
        parsed.netloc,
        path,
        parsed.params,
        parsed.query,
        parsed.fragment
    ))
    
    headers = {"User-Agent": USER_AGENT}
    
    try:
        logger.info(f"Fetching: {url}")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:
            logger.error("Rate limit hit (429). Consider increasing sleep time.")
        else:
            logger.error(f"HTTP error occurred: {e}")
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
    return None

def parse_comments_recursive(
    children: List[Dict[str, Any]], 
    submission_id: str, 
    comments_to_store: List[Tuple[Any, ...]]
):
    """Recursively traverses the Reddit JSON comment tree."""
    for child in children:
        kind = child.get("kind")
        data = child.get("data", {})
        
        if kind == "t1": # This is a comment
            comment_id = data.get("id")
            parent_id = data.get("parent_id", "").split('_')[-1] # Stripping prefix (e.g., t3_ or t1_)
            author = data.get("author", "[deleted]")
            body = data.get("body", "[no text]")
            score = data.get("score", 0)
            depth = data.get("depth", 0)
            permalink = data.get("permalink", "")
            created_utc = data.get("created_utc", 0)
            
            comment_tuple = (
                comment_id,
                submission_id,
                parent_id,
                author,
                body,
                score,
                depth,
                permalink,
                created_utc
            )
            comments_to_store.append(comment_tuple)
            
            # Check for replies
            replies = data.get("replies")
            if isinstance(replies, dict):
                inner_children = replies.get("data", {}).get("children", [])
                parse_comments_recursive(inner_children, submission_id, comments_to_store)
                
        elif kind == "more":
            pass

def scrape_reddit_json(url: str):
    """Main function to scrape a Reddit thread via JSON."""
    data = fetch_json(url)
    if not data or len(data) < 2:
        logger.error("Invalid JSON structure received.")
        return

    # 1. Process Submission Metadata (First element in the list)
    submission_root = data[0].get("data", {}).get("children", [])[0].get("data", {})
    submission_id = submission_root.get("id")
    submission_data = (
        submission_id,
        submission_root.get("title"),
        submission_root.get("url"),
        submission_root.get("subreddit"),
        submission_root.get("created_utc")
    )
    insert_submission(submission_data)
    
    # 2. Process Comments (Second element in the list)
    comment_root_children = data[1].get("data", {}).get("children", [])
    comments_to_store: List[Tuple[Any, ...]] = []
    
    logger.info(f"Parsing comment tree for submission: {submission_id}")
    parse_comments_recursive(comment_root_children, submission_id, comments_to_store)
    
    # 3. Store Comments
    if comments_to_store:
        insert_comments(comments_to_store)
        logger.info(f"Successfully stored {len(comments_to_store)} comments.")

def scrape_subreddit_historical(subreddit: str, target_years: List[int] = [2022, 2023, 2024, 2025], limit_per_year: int = 10):
    """Fetches top posts of all time and filters them by year on the client-side."""
    subreddit = subreddit.strip().replace("r/", "")
    
    # Yearly buckets to store post permalinks
    year_map: Dict[int, List[Dict[str, Any]]] = {year: [] for year in target_years}
    
    after = None
    pages_to_scan = 10 
    
    logger.info(f"--- Scanning Top Posts for r/{subreddit} (Years: {target_years}) ---")
    
    for page in range(pages_to_scan):
        base_url = f"https://www.reddit.com/r/{subreddit}/top.json?t=all&limit=100"
        if after:
            base_url += f"&after={after}"
            
        data = fetch_json(base_url)
        if not data or not isinstance(data, dict):
            break
            
        children = data.get("data", {}).get("children", [])
        if not children:
            break
            
        for child in children:
            post_data = child.get("data", {})
            created_utc = post_data.get("created_utc", 0)
            
            post_year = datetime.datetime.fromtimestamp(created_utc).year
            
            if post_year in year_map and len(year_map[post_year]) < limit_per_year:
                year_map[post_year].append(post_data)
        
        # Check if we have enough for all years
        all_full = all(len(year_map[y]) >= limit_per_year for y in target_years)
        if all_full:
            logger.info("Target limits reached for all years. Stopping search.")
            break
            
        after = data.get("data", {}).get("after")
        if not after:
            break
            
        logger.info(f"Scanning page {page + 2}...")
        time.sleep(2) # Brief sleep between index pages
        
    # Process collected posts
    for year, posts in year_map.items():
        logger.info(f"--- Processing {len(posts)} posts for Year {year} ---")
        for i, post_data in enumerate(posts):
            permalink = post_data.get("permalink")
            if not permalink:
                continue
                
            full_url = f"https://www.reddit.com{permalink}"
            logger.info(f"[{year}] Post {i+1}/{len(posts)}: {post_data.get('title')[:50]}...")
            
            # Scrape the individual thread
            try:
                scrape_reddit_json(full_url)
            except Exception as e:
                logger.error(f"Failed to scrape thread {full_url}: {e}")
            
            delay = random.uniform(5.0, 7.0)
            logger.info(f"Waiting {delay:.2f}s before next post...")
            time.sleep(delay)

if __name__ == "__main__":
    init_db()
    
    parser = argparse.ArgumentParser(description="Reddit Analyzer Phase 2 Scraper")
    parser.add_argument("target", help="Reddit Thread URL or Subreddit Name (e.g., r/test)")
    parser.add_argument("--historical", action="store_true", help="Scrape top posts from 2022-2025 for a subreddit")
    
    args = parser.parse_args()
    
    if args.historical:
        if args.target.startswith("http"):
            logger.error("For --historical mode, please provide a subreddit name (e.g., r/politics) instead of a URL.")
        else:
            scrape_subreddit_historical(args.target)
    else:
        if not args.target.startswith("http"):
            logger.error("Please provide a full Reddit thread URL or use --historical with a subreddit name.")
        else:
            scrape_reddit_json(args.target)
