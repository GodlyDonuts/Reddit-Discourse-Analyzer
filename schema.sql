CREATE TABLE IF NOT EXISTS submissions (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    subreddit TEXT NOT NULL,
    created_utc REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS comments (
    comment_id TEXT PRIMARY KEY,
    submission_id TEXT NOT NULL,
    parent_id TEXT NOT NULL,
    author TEXT,
    body TEXT NOT NULL,
    score INTEGER NOT NULL,
    depth INTEGER NOT NULL,
    permalink TEXT NOT NULL,
    created_utc REAL NOT NULL,
    category TEXT,             
    analysis_reasoning TEXT, 
    sanitized_text TEXT,      
    FOREIGN KEY (submission_id) REFERENCES submissions (id)
);

CREATE INDEX IF NOT EXISTS idx_comments_submission_id ON comments (submission_id);
CREATE INDEX IF NOT EXISTS idx_comments_parent_id ON comments (parent_id);
