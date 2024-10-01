from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import schedule
import time


API_KEY = 'API_key'  
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

def get_youtube_service():
    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

# Setting up MongoDB database
def get_mongodb_client():
    client = MongoClient('mongodb://localhost:27017')  
    return client

def save_to_mongodb(db, collection_name, data):
    collection = db[collection_name]
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

# Extracting Video Data and Comments
def fetch_videos_and_comments(youtube, db, query='#Paris2024', max_results=10):
    search_response = youtube.search().list(
        q=query,
        part='id,snippet',
        maxResults=max_results,
        type='video'
    ).execute()

    videos = []
    comments = []

    for search_result in search_response.get('items', []):
        video_id = search_result['id']['videoId']
        video_title = search_result['snippet']['title']
        video_description = search_result['snippet']['description']

        videos.append({
            'video_id': video_id,
            'title': video_title,
            'description': video_description
        })

        # Extracting comments for the video
        try:
            comment_response = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=max_results
            ).execute()

            for item in comment_response.get('items', []):
                comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                comments.append({'video_id': video_id, 'comment': comment})

        except Exception as e:
            print(f"Error fetching comments for video {video_id}: {str(e)}")

    # Save videos and comments to MongoDB
    save_to_mongodb(db, 'videos', videos)
    save_to_mongodb(db, 'comments', comments)

    return videos, comments

# Main script execution
if __name__ == "__main__":
    youtube_service = get_youtube_service()
    mongodb_client = get_mongodb_client()
    db = mongodb_client['youtube_data']  # Database name

    # Fetch videos and comments, then save to MongoDB
    videos, comments = fetch_videos_and_comments(youtube_service, db, max_results=10)
    print(f"Fetched and saved {len(videos)} videos and {len(comments)} comments to MongoDB.")


    schedule.every(1).hour.do(fetch_videos_and_comments, youtube_service, db, max_results=10)
    while True:
        schedule.run_pending()
        time.sleep(1)
