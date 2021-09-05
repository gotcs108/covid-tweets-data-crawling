from genericpath import isfile
from itertools import tee
import functools

import tweepy

import os
from firebase_admin import firestore
from dotenv import load_dotenv
import json

# For scheduler async event loop
import asyncio

# CONFIG
PROJECT_NAME = 'pro-tracker-325015'
TABLE_NAME = 'tweets'
TWEET_SEARCH_KEYWORDS = ["corona virus", "kung flu", "covid-19", "covid"]
CHECK_NEW_TWEETS_EVEY_X_MIN = 2
# If we can't find a new tweet, wait up to waiting time of 15 minutes (exponential)
WAIT_SEC_LIMIT = 30

# TWEEPY
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__),'dev.env'))
TWEEPY_CONSUMER_KEY = os.getenv('TWEEPY_CONSUMER_KEY')
TWEEPY_CONSUMER_SECRET = os.getenv('TWEEPY_CONSUMER_SECRET')
auth = tweepy.OAuthHandler(TWEEPY_CONSUMER_KEY,
                           TWEEPY_CONSUMER_SECRET)
api = tweepy.API(auth, wait_on_rate_limit_notify=True)

# DB
serivice_account_path = os.path.join(os.path.dirname(__file__),'service_account.json')
if (os.path.isfile(serivice_account_path)):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = serivice_account_path
db = firestore.Client(project=PROJECT_NAME)


@asyncio.coroutine
def upload_tweets():
    run_loop = True

    fetching_mode = fetch_fetching_mode_setting()
    # Verify that we have a fetching mode
    if (not(fetching_mode == 'LISTEN_TO_NEW_TWEETS' or fetching_mode == 'CATCH_UP')):
        raise Exception

    # Keep track of which statuses are caught up (Not synced with the true status we could find from the DB)
    caught_up_status_keyword = {}
    for keyword in TWEET_SEARCH_KEYWORDS:
        caught_up_status_keyword[keyword] = False

    # Fetch the oldest or the newest tweet id for each keyword depending on the fetching mode
    id_limit_cache = fetch_id_to_dict(TWEET_SEARCH_KEYWORDS, fetching_mode)

    print("Starting the fetch task loop.")
    counter = 0
        
    while run_loop:
        print(f'Fetch loop count: {counter}')
        # Catch Rate Limit Error. If we exceeded the rate limit, sleep for 15 minutes
        # try:
        for keyword in TWEET_SEARCH_KEYWORDS:
            print(f'Loop {counter}: Fetching keyword "{keyword}"')

            # Build parameter to search tweets and search
            args = [api.search]
            kwargs= {'lang':'en', 'q':f'{keyword} place:3376992a082d67c7'}
            if (id_limit_cache[keyword] is not None):
                if (fetching_mode == 'LISTEN_TO_NEW_TWEETS'):
                    kwargs['min_id'] = id_limit_cache[keyword]+1
                else:
                    kwargs['max_id'] = id_limit_cache[keyword]-1
            tweet_search = tweepy.Cursor(*args, **kwargs).items(1)

            # If there is no result anymore, wait up to X minutes. We only wait in ONLY NEW TWEETS mode. If a keyword is all caught up, we raise a flag.
            tweet_search, empty_test = tee(tweet_search)
            try:
                next(empty_test)
            except StopIteration:
                if (fetching_mode == 'CATCH_UP'):
                    # Listen to new tweets if all keywords are caught up
                    caught_up_status_keyword[keyword] = True
                    all_caught_up = functools.reduce(lambda acc, value: acc and value, list(caught_up_status_keyword.values()))
                    print(f'{keyword} is caught up. Moving to the next keyword.')

                    if (all_caught_up):
                        print(f'All keywords are caught up. Start listening to new tweets.')
                        fetching_mode = 'LISTEN_TO_NEW_TWEETS'
                        # Save the new fetching mode to the DB
                        settings_doc_ref = db.collection(u'settings').document()
                        settings_doc_ref.set({"settings_name": "fetching_mode", "value": "LISTEN_TO_NEW_TWEETS"}, merge=True)
                else:
                    wait_sec = 1
                    while (wait_sec <= WAIT_SEC_LIMIT):
                        print(f'No results for {keyword}. Waiting {wait_sec} seconds up to {WAIT_SEC_LIMIT} seconds.')
                        yield from asyncio.sleep(wait_sec)
                        wait_sec *= 2
        
            # Upload the fetched tweets
            for tweet_status in tweet_search:
                id_limit_cache[keyword] = tweet_status.id

                # Read from the cursor
                tweet_json = tweet_status._json

                # Insert the tweet to the db collection
                tweets_doc_ref = db.collection('tweets').document()
                tweets_doc_ref.set({"id": tweet_json["id"],
                                "text": tweet_json["text"],
                                "created_at": tweet_json["created_at"],
                                "place": json.dumps(tweet_json["place"]),
                                "keyword": keyword
                                })
                print(f'Inserted tweet for {keyword} with id {tweet_json["id"]}.')
            
            # Sleep after each keyword fetch
            print(f'Loop {counter}: Completed fetching keyword "{keyword}"')
            yield from asyncio.sleep(1)
    # except:
        # To handle rate limit error, sleep 15 minutes (try it every 5 minutes)
        # print("Rate Limit Error")
        # yield from asyncio.sleep(300)
    # finally:
        counter += 1

def fetch_id_to_dict(keywords, fetching_mode):
    id_limit_dict = {}

    for keyword in keywords:
        if fetching_mode == 'LISTEN_TO_NEW_TWEETS':
            tweet_id = fetch_most_recent_tweet_id(keyword)
            print(f'The latest tweet id for {keyword}: {tweet_id}')
        elif fetching_mode == 'CATCH_UP':
            tweet_id = fetch_oldest_tweet_id(keyword)
            print(f'The oldest tweet id for {keyword}: {tweet_id}')
        else:
            # There is no set fetching mode
            raise Exception

        if (tweet_id):
            id_limit_dict[keyword] = tweet_id
        else:
            id_limit_dict[keyword] = None

    return id_limit_dict


def fetch_most_recent_tweet_id(keyword):
    tweets_doc_ref = db.collection(u'tweets')
    query = tweets_doc_ref.where('keyword', '==', keyword).order_by(
        u'id', direction=firestore.Query.DESCENDING).limit(1)
    results = query.stream()
    
    try:
        result = next(results)
    except StopIteration:
        return None

    return result.to_dict()['id']

def fetch_oldest_tweet_id(keyword):
    tweets_doc_ref = db.collection(u'tweets')
    query = tweets_doc_ref.where('keyword', '==', keyword).order_by(
        u'id', direction=firestore.Query.ASCENDING).limit(1)
    results = query.stream()

    try:
        result = next(results)
    except StopIteration:
        return None

    return result.to_dict()['id']

# Fetch settings from the DB to see if the fetching mode is in "LISTEN_TO_NEW_TWEETS" or "CATCH_UP". Default is "CATCH_UP".
def fetch_fetching_mode_setting():
    settings_doc_ref = db.collection(u'settings')
    query = settings_doc_ref.where('settings_name', '==', 'fetching_mode').limit(1)
    results = query.stream()

    try:
        result = next(results)
    except StopIteration:
        return 'CATCH_UP'

    return result.to_dict()['value']

# Delete all documents in all collections (settings and tweets) except for the indexes
def reset_db():
    tweets_doc_ref = db.collection(u'tweets')
    for doc in tweets_doc_ref.stream():
        doc.reference.delete()
    settings_doc_ref = db.collection(u'settings')
    for doc in settings_doc_ref.stream():
        doc.reference.delete()

# Set up the event loop
loop = asyncio.get_event_loop()
print("Starting the event loop and adding the fetch task.")
task = loop.run_until_complete(upload_tweets())