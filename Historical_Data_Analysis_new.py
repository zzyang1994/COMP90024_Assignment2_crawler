from operator import index
from pickle import NONE
from turtle import setundobuffer
from matplotlib.pyplot import get
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import numpy as np
from textblob import TextBlob
import json
import couchdb
import subfinder
from datetime import datetime
import os
from concurrent.futures import ThreadPoolExecutor
import threading
import re
import nltk
nltk.download('punkt')
nltk.download('stopwords')


# couchDB
couchDB_server = 'http://admin:password@115.146.95.136:5984/'
db = ['transportationxm_historical', 'healthxm_historical']
JSONPATH = 'twitter-melb.json'
RULESPATH = "Rules.json"
THREADNUMBER = 4
couch = couchdb.Server(couchDB_server)
db_t = couch['transportationxm_historical']
db_h = couch['healthxm_historical']
NEEDSUB = ["Wyndham", "Melton", "Casey", "Hume", "Cardinia", "Maribyrnong", "Frankston", "Whittlesea", "Brimbank",
        "Yarra", "Whitehorse", "Stonnington", "Melbourne", "Port Phillip", "Banyule", "Glen Eira", "Moonee Valley",
        "Boroondara", "Manningham", "Bayside"]


# Search tweets with query
class query_finder():
    def __init__(self):
        self.query_dict = {}
        with open(RULESPATH, 'r') as f:
            querys = json.load(f)
            for query_title in querys:
                for i in range(len(querys[query_title])):
                    try:
                        self.query_dict[f'{query_title}:q{i+1}'] = [
                            self.get_query(j) for j in querys[query_title]
                            [f'q{i + 1}'].split(' OR ')
                        ]
                    except BaseException as e:
                        print("get_query function error: ", e)
                        raise

    def get_query(self, x):
        if x[0] == '"':
            return x[1:-1]
        return x

    # find tweets meet the query
    def query_from(self, text):
        match = False
        for i in self.query_dict.keys():
            for j in self.query_dict[i]:
                if j in text:
                    match = True
                    return {'match': match, 'query': i.split(':')}
        return {'match': match, 'query': None}


# Split file into 4 chunks
def Splitfile():
    with open(JSONPATH, 'r', encoding='utf-8') as f:
        file_size = os.path.getsize(JSONPATH)
        chunk_estimated_size = file_size / THREADNUMBER
        chunk_start = []
        chunk_end = []
        f.readline()  # ignore the first line
        while (True):
            current_start = f.tell()
            chunk_start.append(current_start)
            f.seek(current_start + chunk_estimated_size)
            f.readline()
            current_end = f.tell()
            if current_end >= file_size:
                current_end = file_size
                chunk_end.append(current_end)
                return chunk_start, chunk_end
            chunk_end.append(current_end)
            f.seek(current_end)


# find address with coordinates
def get_address(tweets, addresssearcher):
    # find the coordinates
    coordinate = []
    if tweets['doc']['coordinates']:
        coordinate = tweets['doc']['coordinates']['coordinates']
    else:
        if tweets['doc']['place']:
            bounding_box = tweets['doc']['place']['bounding_box'][
                'coordinates']
            longitude = (bounding_box[0][0] + bounding_box[1][0] +
                         bounding_box[2][0] + bounding_box[3][0]) / 4
            latitude = (bounding_box[0][1] + bounding_box[1][1] +
                        bounding_box[3][1] + bounding_box[3][1]) / 4
            coordinate = [longitude, latitude]
        else:
            return
    # bulid coorditaed string
    latitude = coordinate[1]
    longitude = coordinate[0]
    try:
        geoinfo = addresssearcher.GetPlace(latitude, longitude)
    except BaseException as e:
        # print("cannot find place: ", e)
        return
    # return address
    try:
        state = geoinfo["state"]
        town = geoinfo["suburb"]
        city = "Greater Melbourne"  # geoinfo["city"]
        address = {"city": city, "suburb": town, 'coordinates': coordinate}
        return address
    except Exception:
        print("cannot correctly return address", e)
        return {
            "city": "In the Sea or Unknown area",
            "suburb": "SomeWhere Victoria",
            'coordinates': coordinate
        }


# sentiment analysis to tag each tweet with a sentiment score, from -1 to 1
def sentiment(tweets):

    # clean text
    text = tweets.lower()
    text = re.sub(r'(@[A-Za-z0-9_]+)', '',
                  text)  # Removes all mentions (@username)
    text = re.sub('http://\S+|https://\S+', '',
                  text)  # Removes any link in the text
    text = re.sub(r'[^\w\s]', '', text)  # Basically removes punctuation
    text_tokens = word_tokenize(
        text)  # Removes stop words that have no use in sentiment analysis
    text = [word for word in text_tokens if word not in stopwords.words()]

    tweets = ' '.join(text)

    # find sentiments of the tweets
    testimonial = TextBlob(tweets)
    sent = testimonial.sentiment

    # whether positive or negative
    polarity = sent.polarity

    # whether subjective or objective
    subjective = sent.subjectivity
    if 0 < subjective <= 0.5:
        attitude = "objective"
    else:
        attitude = "subjective"
    return [polarity, attitude, tweets]


# find the popular index of the tweets
def popularIndex(retweet, favourite):
    if retweet + favourite != 0:
        index = 0.6 * retweet + 0.4 * favourite
    else:
        index = 0
    return index

# get importance 
def importance(polarity, popular_index):
    imp = polarity*popular_index/10000
    return imp

# save data to couchDB
def saveData(tweets, tweets_process, db):
    date = datetime.strptime(tweets['doc']['created_at'], '%a %b %d %H:%M:%S %z %Y')
    mon = date.strftime("%b")
    imp = importance(tweets_process['sentiments'], tweets_process['favouriteIndex'])
    if db == "t":
        db_t.save({
            '_id': tweets["id"],
            'sentiments': tweets_process['sentiments'],
            'attitude': tweets_process['attitude'],
            'longitude': tweets_process['coordinates'][0],
            'latitude': tweets_process['coordinates'][1],
            'suburb': tweets_process['suburb'],
            'city': tweets_process['city'],
            'importance': imp,
            'truncated': tweets['doc']['truncated'],
            'Related to': tweets_process['related_to'],
            'text': tweets['doc']['text'],
            'popular_index': tweets_process['favouriteIndex'],
            'created_at_year': date.year,
            'created_at_month': mon,
            'created_at_day': date.day,
        })
    elif db == "h":
        db_h.save({
            '_id': tweets["id"],
            'sentiments': tweets_process['sentiments'],
            'attitude': tweets_process['attitude'],
            'longitude': tweets_process['coordinates'][0],
            'latitude': tweets_process['coordinates'][1],
            'suburb': tweets_process['suburb'],
            'city': tweets_process['city'],
            'importance': imp,
            'truncated': tweets['doc']['truncated'],
            'Related to': tweets_process['related_to'],
            'text': tweets['doc']['text'],
            'text_cleaned': tweets_process['text_cleaned'],
            'popular_index': tweets_process['favouriteIndex'],
            'created_at_year': date.year,
            'created_at_month': mon,
            'created_at_day': date.day,
        })


# remove , and \n in the end of current line
def FormatJson(line):
    if line[-2] == ",":
        line = line[:-2]
    elif line[-2] == '}' and line[-3] == ']':
        line = line[:-3]
    return line


# Process data
def AnalysisHD(cst, cend):
    try:
        finder = query_finder()
    except BaseException:
        print("finder init error")
        raise
    addresssearcher = subfinder.subfinder()
    with open(JSONPATH, 'r', encoding='utf-8') as f:
        f.seek(cst)
        try:
            while (f.tell() < cend):
                line = f.readline()
                if not line:
                    break
                line = FormatJson(line)
                try:
                    tweets = json.loads(line)
                except BaseException as e:
                    continue
                tweets_process = {}
                text = tweets['doc']['text']
                try:
                    result = finder.query_from(text)
                except Exception as e:
                    print("query_from function error: ", e)
                    raise
                # find loactions
                try:
                    address = get_address(tweets, addresssearcher)
                    tweets_process['suburb'] = address['suburb']
                    tweets_process['city'] = address['city']
                    tweets_process['coordinates'] = address['coordinates']
                except Exception as e:
                    continue
                if result['match']:
                    # find sentiments
                    try:
                        sen = sentiment(text)
                        tweets_process['sentiments'] = sen[0]
                        tweets_process['attitude'] = sen[1]
                        tweets_process['text_cleaned'] = sen[2]
                    except BaseException as e:
                        print("sentiment function not work: ", e)
                    # find the popular index
                    retweet_count = tweets['doc']['retweet_count']
                    favourite_count = tweets['doc']['user']['favourites_count']
                    try:
                        popularindex = popularIndex(retweet_count, favourite_count)
                        tweets_process['favouriteIndex'] = popularindex
                    except BaseException as e:
                        print("popularindex function not work: ", e)
                    # save to couchDB
                    tweets_process['related_to'] = result["query"][1]
                    if result["query"][0] == 'Query_Transportation':
                        try:
                            if tweets_process["suburb"] in NEEDSUB:
                                saveData(tweets, tweets_process, "t")
                        except BaseException:
                            pass
                    else:
                        try:
                            if tweets_process["suburb"] in NEEDSUB:
                                saveData(tweets, tweets_process, "h")
                        except BaseException:
                            pass
            if (f.tell() >= cend):
                print("finish chunk")
        except BaseException as e:
            print("cannot correctly process chunk ", e)


if __name__ == "__main__":
    chunk_starts, chunk_ends = Splitfile()
    try:
        with ThreadPoolExecutor(max_workers=THREADNUMBER) as pool:
            for i in range(THREADNUMBER):
                pool.submit(AnalysisHD, chunk_starts[i],
                            chunk_ends[i])  # 提交启动线程的请求到线程池
    except BaseException:
        print("cannot start threads")
        raise
