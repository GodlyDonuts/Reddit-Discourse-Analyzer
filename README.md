# Reddit Discourse Analyzer

An AI-powered research tool designed for high-speed scraping, sanitization, and deep qualitative coding of Reddit political discourse. This pipeline enables researchers to process thousands of comments, redact sensitive information, and categorize hostility levels using state-of-the-art Large Language Models.

## 🚀 Key Features

*   **High-Speed Scraping**: Efficiently collects top posts and historical data (2022–2025) from any subreddit.
*   **AI-Driven Sanitization**: Automatically redacts Personally Identifiable Information (PII) and filters out non-substantive noise.
*   **Deep Qualitative Coding**: Categorizes comments into hostility levels (*Neutral*, *Political Critique*, *Borderline*, *Dehumanization*) with linguistic reasoning provided for every label.
*   **Gemini 1.5 Integration**: Leverages Google's Gemini 3.1 Flash-Lite for one-shot, batch-optimized analysis (200 comments per request).
*   **High Throughput**: Capable of processing 18,000+ comments in under 10 minutes at 14 RPM.
*   **Research-Ready Storage**: Maintains a structured SQLite database for persistent analysis and resumption.

## 🛠️ Setup & Installation

### 1. Prerequisites
*   Python 3.10+
*   Google Gemini API Key
*   Reddit App Credentials (Client ID, Secret, User Agent)

### 2. Installation
Clone the repository and install dependencies:
```bash
git clone https://github.com/your-username/reddit-discourse-analyzer.git
cd reddit-discourse-analyzer
pip install -r requirements.txt
```

### 3. Configuration
Create a `.env` file in the root directory (see `.env.example`):
```ini
GEMINI_API_KEY=your_gemini_api_key
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=your_user_agent
```

## 📈 Usage

### Phase 1: Data Collection
Scrape a specific thread or historical subreddit data:
```bash
# Scrape a single thread
python3 reddit_scraper.py "https://reddit.com/r/politics/..."

# Scrape top historical posts (2022-2025)
python3 reddit_scraper.py r/politics --historical
```

### Phase 2: Analysis & Deep Coding
Run the unified analyzer to sanitize and categorize comments:
```bash
python3 hybrid_analyzer.py
```

### Phase 3: Monitoring
Check the status of your research database:
```bash
python3 check_db.py
```

## 🔬 Research Methodology

The tool uses a **One-Shot Analysis Pipeline** to perform:
1.  **PII Redaction**: Strips real names, phone numbers, and addresses.
2.  **Substantive Filtering**: Keeps only discourse relevant to policy or institutional critique.
3.  **Hostility Coding**:
    *   **Neutral**: Objective or civil discourse.
    *   **Political Critique**: Critiques of policy or ideological positions.
    *   **Borderline**: Use of tropes or coded language.
    *   **Dehumanization**: Essentializing or stripping humanity from groups.

## ⚖️ Disclaimer
This tool is intended for academic research purposes. Users are responsible for complying with the Reddit API Terms of Service and ensuring ethical data handling practices.
