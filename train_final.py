import pandas as pd
import emoji
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline

# Loading XLM Roberta model
tokenizer = AutoTokenizer.from_pretrained("cardiffnlp/twitter-xlm-roberta-base-sentiment")
model = AutoModelForSequenceClassification.from_pretrained("cardiffnlp/twitter-xlm-roberta-base-sentiment")

# Creating Sentiment Analysis Pipeline
sentiment_analysis = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)

# Load the comment dataset
data = pd.read_json('youtube_data.comments.json')

print("Initial Data:")
print(data.head())  
print("Columns in DataFrame:", data.columns.tolist()) 

# Preprocessing the data
def process_comment(comment):
    return emoji.demojize(comment)  # Convert emojis to their meaningful text form


def analyze_sentiment(comment):
    result = sentiment_analysis(comment, truncation=True, max_length=512)[0]  # Ensures truncation
    print(f"Analyzing sentiment for: {comment}")
    print(f"Result: {result}")
    return result['label'], result['score']

# Analyze sentiment for comment_text and comment_emoji
data[['text_sentiment', 'text_score']] = data['comment_text'].apply(analyze_sentiment).apply(pd.Series)
data[['emoji_sentiment', 'emoji_score']] = data['comment_emoji'].apply(analyze_sentiment).apply(pd.Series)

# Combine sentiments for aggregation
data['combined_sentiment'] = data.apply(lambda x: x['text_sentiment'] if x['text_score'] >= x['emoji_score'] else x['emoji_sentiment'], axis=1)
data['combined_score'] = data[['text_score', 'emoji_score']].max(axis=1)

# Group by video_id and aggregate sentiments
aggregated_data = data.groupby('video_id').agg({
    'combined_sentiment': 'first',  
    'combined_score': 'mean'  
}).reset_index()

# Show aggregated results
print("\nAggregated Sentiment Analysis Results:")
print(aggregated_data)

# Export data to CSV for Tableau visualization
aggregated_data.to_csv('aggregated_youtube_comments.csv', index=True)