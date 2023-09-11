from pymongo import MongoClient
import json

# Connects to the MongoDB database
client: MongoClient = MongoClient('127.0.0.1', 27017)
db = client['local']
tweet = db['Tweets']

# Opens a new JSON file to write the cleaned tweets to
with open('Cleaned.json', 'w', encoding='utf8') as output:
    output.write('[')
    # Iterates over all the tweets in the MongoDB collection
    for t in tweet.find():
        # ================================================Tweet==================================================================
        # Extracts the user location, if available
        if 'Location' in t['actor']:
            userlocation = t['actor']['Location']['displayName']

        # Extracts the user summary, if available
        if 'summary' in t['actor']:
            summary = t['actor']['summary']
            if summary is None:
                summary = 'N/A'

        if "profileLocations" in t["gnip"]:
            place = t["gnip"]["profileLocations"][0]["address"]["country"]

        tweet_hashtags = []
        url_list = []
        user_mentions = []

        # Extracts the tweet hashtags
        if 'hashtags' in t['twitter_entities']:
            hashtags = t["twitter_entities"]["hastags"]
            for h in hashtags:
                hashtag = h["text"]
                tweet_hashtags.append(hashtag)

        if "urls" in t["twitter_entities"]:
            urls = t["twitter_entities"]["urls"]
            for u in urls:
                urls_id = u["url"]
                url_list.append(urls_id)

        if "user_mentions" in t["twitter_entities"]:
            mentions = t["twitter_entities"]["user_mentions"]
            for m in mentions:
                user = m["screen_name"]
                user_mentions.append(user)

        # ===========================================Retweet=====================================================================
        retweet_hashtags = []
        retweet_url_list = []
        retweet_user_mentions = []

        if "actor" in t["object"]:
            if "location" in t["object"]["actor"]:
                retweet_userlocation = t["object"]["actor"]["location"]["displayName"]
            if "summary" in t["object"]["actor"]:
                retweet_summary = t["object"]["actor"]["summary"]
                if retweet_summary is None:
                    retweet_summary = "N/A"

        if "twitter_entities" in t['object']:
            hashtags = t["object"]["twitter entities"]["hashtags"]
            for h in hashtags:
                hashtag = h["text"]
                retweet_hashtags.append(hashtag)

            urls = t['object']["twitter_entities"]["urls"]
            for u in urls:
                urls_id = u["url"]
                retweet_url_list.append(urls_id)

            mentions = t["object"]["twitter_entities"]["user_mentions"]
            for a in mentions:
                user = ["screen_name"]
                retweet_user_mentions.append(user)

        # =======================================================================================================================

        if t["verb"] == "post":  # Tweet
            tweet = {
                "created_at": t['postedTime'],
                "id_str": t['id'],
                "text": t['text'],
                "user": {"id": t["actor"]["id"],
                         "name": t["actor"]["preferredUsername"],
                         "screen_name": t["actor"]["displayName"],
                         "Location": userlocation,
                         "url": t["actor"]["link"],
                         "description": summary,
                         },
                "Languages": t["twitter_Lang"],
                "retweetCount": t["retweetCount"],
                "favoritesCount": t["favoritesCount"],
                "place": place,
                "generator": t["generator"]["displayName"],
                "entities": {"hashtags": tweet_hashtags,
                             "urls": url_list
                             },
                "user_mentions": user_mentions,
                "retweet_id": "",
                "retweet_status": "N"
            }
        else:  # Retweet
            tweet = {
                "created_at": t['postedTime'],
                "id_str": t['id'],
                "text": t['text'],
                "user": {
                    "id": t["actor"]["id"],
                    "name": t["actor"]["preferredUsername"],
                    "screen_name": t["actor"]["displayName"],
                    "Location": userlocation,
                    "url": t["actor"]["link"],
                    "description": summary,
                },
                "Languages": ["twitter_lang"],
                "retweetCount": t["retweetCount"],
                "favoritesCount": t["favoritesCount"],
                "place": place,
                "generator": t["generator"]["displayName"],
                "entities": {
                    "hashtags": tweet_hashtags,
                    "urls": url_list
                },
                "user_mentions": user_mentions,
                "retweet_id": t['object']['id'],  # The original tweet id of this retweet
                "retweet_status": "Y"
            }

        json_object = json.dumps(tweet, indent=4)
        output.write(json_object)

        # ==========================================EXTRACT Retweets =============================================================================

        if t["verb"] == "post":  # Tweet
            tweet = {}  # If it is a tweet create a blank JSON object i.e {} then delete {} later

        # If it is a retweet, then extract retweet information
        if t["verb"] == "share":  # Retweet
            tweet = {
                "created_at": t['object']['postedTime'],
                "id_str": t['object']['id'],
                "text": t['object']['text'],
                "user": {
                    "id": t['object']["actor"]["id"],
                    "name": t['object']["actor"]["preferredUsername"],
                    "screen_name": t['object']["actor"]["displayName"],
                    "Location": "retweet_userlocation",  # Replace with actual data
                    "url": t['object']["actor"]["link"],
                    "description": "retweet_summary",  # Replace with actual data
                },
                "Languages": [t["twitter_lang"]],
                "retweetCount": t["retweetCount"],
                "favoritesCount": t['object']["favoritesCount"],
                "generator": t['object']["generator"]["displayName"],
                "entities": {
                    "hashtags": "retweet_hashtags",  # Replace with actual data
                    "urls": "retweet_url_list"  # Replace with actual data
                },
                "user_mentions": "retweet_user_mentions",  # Replace with actual data
                "retweet_id": "",  # Replace with actual data
                "retweet_status": "RT"
            }

        json_object2 = json.dumps(tweet, indent=4)
        output.write(json_object2)
    output.write("]")
output.close()

# Clean JSON file
with open("Cleaned.json", "r+", encoding="utf8") as file:
    string = file.read()
    file.seek(0)
    string = string.replace('', '}, \n{"')
    string = string.replace(' ', 'in')
    file.write(string)

file.close()
