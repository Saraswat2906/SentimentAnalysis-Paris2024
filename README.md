# YouTube Comment Sentiment Analysis using XLM-RoBERTa

This project performs **multilingual sentiment analysis** on YouTube comments using the `cardiffnlp/twitter-xlm-roberta-base-sentiment` model. It extracts videos and comments using the YouTube Data API, stores them in MongoDB, splits comments into text and emoji, and analyzes sentiment for both. The results are then aggregated and exported for visualization (e.g., Tableau).

---

## 🔍 Project Overview

- Extracts YouTube videos and top-level comments based on a query (e.g., `#Paris2024`).
- Splits comments into **text** and **emoji** parts.
- Analyzes sentiment for both using XLM-RoBERTa.
- Stores results in MongoDB and exports to CSV for dashboarding.
- Designed for **scheduled, automatic updates** using the `schedule` module.

---

## 📁 Project Structure

```plaintext
project-root/
│
├── main_sentiment_pipeline.py       # Sentiment analysis + CSV export
├── youtube_data_fetcher.py          # Fetches video & comment data from YouTube API and saves to MongoDB
├── comment_splitter.py              # Splits comment into text and emoji, updates MongoDB
├── requirements.txt                 # All required Python packages
├── README.md                        # Project documentation (this file)
├── youtube_data.comments.json       # (Optional) JSON dump of comments for local testing
└── aggregated_youtube_comments.csv  # Output for visualization

```
## 📄 File Descriptions

### `main_sentiment_pipeline.py`
- Loads data from `youtube_data.comments.json`
- Uses Hugging Face pipeline to analyze sentiment on:
  - `comment_text` (words)
  - `comment_emoji` (emoji meanings)
- Aggregates sentiment per video and exports to `aggregated_youtube_comments.csv`

### `youtube_data_fetcher.py`
- Uses YouTube Data API v3 to search for videos and extract top-level comments
- Saves videos and comments to a local MongoDB instance
- Runs hourly using `schedule` for periodic updates

### `comment_splitter.py`
- Splits each comment into:
  - `comment_text`: alphanumeric words
  - `comment_emoji`: only emoji characters
- Updates the MongoDB `comments` collection with the split fields

### `requirements.txt`
- Lists all required libraries and packages to run the project

---

### 'Install Requirements'
- Install required packages using pip install -r requirements.txt

