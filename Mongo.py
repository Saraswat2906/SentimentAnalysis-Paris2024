from pymongo import MongoClient
import re
import emoji

# MongoDB connection-
client = MongoClient("mongodb://localhost:27017")
db = client['youtube_data']
comment_collection = db['comments']

# Split text and emojis
def split_text_and_emoji(comment):
    text = re.sub(r'[^\w\s]', '', comment)  
    emojis = ''.join(c for c in comment if emoji.is_emoji(c))  
    text = ''.join(c for c in comment if not emoji.is_emoji(c))  
    return text.strip(), emojis.strip()


comments = comment_collection.find()


for comment_doc in comments:
    comment = comment_doc['comment']
    comment_text, comment_emoji = split_text_and_emoji(comment)
    print(f"Comment Text: {comment_text}")
    print(f"Comment Emoji: {comment_emoji}")
    # Update the document with new fields 'comment_text' and 'comment_emoji'
    comment_collection.update_one(
        {'_id': comment_doc['_id']},
        {'$set': {'comment_text': comment_text, 'comment_emoji': comment_emoji}}
    )

print("Comments have been split into text and emoji parts and saved in the database.")
