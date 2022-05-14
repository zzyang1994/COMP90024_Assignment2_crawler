import re 
import nltk
import pandas as pd
import matplotlib.pyplot as plt
nltk.download('punkt')   
nltk.download('stopwords')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from textblob import TextBlob
import random
def rand():
    a = random.randint(-1000,1000)
    return a/100000

# sentiment analysis to tag each tweet with a sentiment score, from -1 to 1
def sentiment(tweets):
    
    # clean text
    text = tweets.lower()
    text = re.sub(r'(@[A-Za-z0-9_]+)', '', text) # Removes all mentions (@username)
    text = re.sub('http://\S+|https://\S+', '', text)  # Removes any link in the text
    text = re.sub(r'[^\w\s]', '', text) # Basically removes punctuation
    text_tokens = word_tokenize(text) # Removes stop words that have no use in sentiment analysis 
    text = [word for word in text_tokens if not word in stopwords.words()]

    tweets = ' '.join(text)

    # find sentiments of the tweets
    testimonial = TextBlob(tweets)
    sent = testimonial.sentiment

    # whether positive or negative
    polarity = sent.polarity
    if polarity == 0:
        polarity = rand()
    # whether subjective or objective
    subjective = sent.subjectivity
    if 0 < subjective <= 0.5:
        attitude = "objective"
    else:
        attitude = "subjective"
    return [polarity , attitude]

# longitude: 144.28232456 + latitude: -36.737158501500005


from os import stat
from types import CoroutineType
from geopy.geocoders import Nominatim
import time

# find address with coordinates


def get_address(coordinate):

    geolocator = Nominatim(user_agent="geopiExercise")

    # bulid coorditaed string
    latitude = str(coordinate[0])
    longitude = str(coordinate[1])
    coordinates = f"{latitude}, {longitude}"

    # sleep for a second to respect Usage Policy
    time.sleep(0.6)
    try:
        location = geolocator.reverse(coordinates).raw
    except:
        get_address(coordinate)

    # return address
    city = location.get('city')
    county = location.get('county', '')
    town = location.get('suburb', )
    boundingbox = location.get('boundingbox')
    address = {"city": city, "county": county,
               "town": town, "boundingbox": boundingbox}

    try:
        return location["address"]["suburb"], location["address"]["city"]
    except:
        return "In the Sea or Unknown area", "SomeWhere Victoria"

import json
import requests


def twitter_request(query, location_range, auth_headers, count, max_id=2**64,since_id=0):
    url = "https://api.twitter.com/1.1/search/tweets.json"
    params = {"q": query, "geocode": location_range,
              "count": count, "max_id": max_id,"since_id":since_id}
    response = requests.get(url=url, params=params, headers=auth_headers)
    json_data = response.json()
    json_data["auth_headers"] = auth_headers
    return json_data
    pass


def process_response(response_from_twapi):
    # try:
    #     pass
    # print(response_from_twapi)
    try:
        data = response_from_twapi['statuses']
        if len(data)== 0:
            return [],-1 
        last_tweet_id = data[len(data)-1]["id"]
        first_id = data[0]["id"]
    except:
        try:
            print(response_from_twapi)
        except BaseException as e:
            print(e)
    # except:
    #     print("exception!")
    #     return
    return data, last_tweet_id,first_id
    pass


def exact_time(str_time):
    # Month,Day,Year
    try:
        list = str_time.split(" ")
        return list[1],list[2],list[-1]
    except:
        return "April",11,2022

def process_tweets(tweets, suburb="None", city="None", longitude=-1, latitude=-1, related_to="None"):
    def importance(polariy, popular_index):
        if popular_index == 0:
            importance = 0.1/10000
        else:
            importance = polariy*popular_index/10000
        return importance
    
    json_list = []
    for tweet in tweets:
        # not_null_spe_coordinate = tweet["coordinates"] != None
        # not_null_approx_coordinate = tweet["place"] != None
        # if not_null_spe_coordinate:
        if True:
            text = tweet["text"]
            id = tweet["id_str"]
            popular_index = tweet["retweet_count"] * \
                0.6 + tweet["favorite_count"]*0.4
            month,day,year = exact_time(tweet["created_at"])
            sentiments, attitude = sentiment(text)

            importance1 = importance(sentiments,popular_index)
            # longitude = tweet["coordinates"]["coordinates"][0]
            # latitude = tweet["coordinates"]["coordinates"][1]
            created_at = tweet["created_at"]
            # suburb, city = get_address([latitude, longitude])
            truncated = tweet["truncated"]
            json_data = {"id": id, "sentiments": sentiments, "attitude": attitude, "longitude": longitude, "latitude": latitude,
                         "suburb": suburb, "city": city, "importance": importance1, "truncated": truncated, "related_to": related_to, "text": text, "popular_index": popular_index, "created_at_year": year,"created_at_month": month,"created_at_day": day}
            json_list.append(json_data)
            # print(f"sentiments:{sentiments} + attitude:{attitude}")
            # print(f"longitude: {longitude} + latitude: {latitude}")
            # print(f"suburb: {suburb} + city: {city}")
            # print("\n")
            pass
        # elif not_null_approx_coordinate:
        #     text = tweet["text"]
        #     id = tweet["id"]
        #     truncated = tweet["truncated"]
        #     created_at = tweet["created_at"]
        #     popular_index = tweet["retweet_count"]*2 + tweet["favorite_count"]
        #     sentiments, attitude = sentiment(text)
        #     longitude = (tweet["place"]["bounding_box"]["coordinates"][0][0][0] + tweet["place"]["bounding_box"]["coordinates"][0][1]
        #                  [0] + tweet["place"]["bounding_box"]["coordinates"][0][2][0] + tweet["place"]["bounding_box"]["coordinates"][0][3][0])/4
        #     latitude = (tweet["place"]["bounding_box"]["coordinates"][0][0][1] + tweet["place"]["bounding_box"]["coordinates"][0][1]
        #                 [1] + tweet["place"]["bounding_box"]["coordinates"][0][2][1] + tweet["place"]["bounding_box"]["coordinates"][0][3][1])/4
        #     suburb, city = get_address([latitude, longitude])
        #     json_data = {"id": id, "created_at": created_at, "sentiments": sentiments, "attitude": attitude, "longitude": longitude,
        #                  "latitude": latitude, "suburb": suburb, "city": city, "truncated": truncated, "text": text, "popular_index": popular_index}
        #     json_list.append(json_data)
        #     # print(f"sentiments:{sentiments} + attitude:{attitude}")
        #     # print(f"longitude: {longitude} + latitude: {latitude}")
        #     # print(f"suburb: {suburb} + city: {city}")
        #     # print("\n")
        #     pass
        # else:
        #     continue
    return json_list
    pass
# longitude: 144.369464016 + latitude: -38.132400992


import couchdb


def insert_couchdb(couchDB_server, db_name, json_list):
    # print("couchdb inserting")
    couch = couchdb.Server(couchDB_server)
    results = []
    try:
        db = couch.create(db_name)
    except Exception as e:
        # print(e)
        pass
    db = couch[db_name]
    for json in json_list:
        try:
            id, _rev = db.save(json)
            one_result = (id, _rev)
        except BaseException as ed:
            one_result = f"entry whose id {json} insert failed"
            print("Inserting exactly")
            print(ed)
        results.append(one_result)




def singlekey_multi_request(key_bearer, query, location_range, db_name, round=90, max_id=2**64,suburb="None",city="None",latitude = -1, longitude = -1,related_to="None",since_id=0):
    key_bearer = "Bearer "+key_bearer
    key_bearer = {"Authorization": key_bearer}
    total_get = 0
    i=0
    while total_get<4000:
        i += 1
        # print(f"round: {i}")
        r = twitter_request(query=query, auth_headers=key_bearer,
                            location_range=location_range, max_id=max_id, count=80,since_id=since_id)
        try:
            tweets, last_min_id_raw,first_id = process_response(r)
            if last_min_id_raw == -1:
                break
            if len(tweets) == 0:
                break
            
            last_min_id = last_min_id_raw
            total_get += len(tweets)
        except:
            print("null tweet list")
            break
        try:
            json_list = process_tweets(tweets=tweets,suburb=suburb,city=city,latitude=latitude,longitude=longitude,related_to = related_to)

            insert_couchdb(couchDB_server=couchdb_url,
                           db_name=db_name, json_list=json_list)
        except:
            print("Exception in insert_couchdb")
            break
        # tweets processing
        max_id = last_min_id - 1
        print(f"total: {total_get}\n")
    return max_id
    pass

import time
import math
def radius(area):
    return math.sqrt(area)*math.sqrt(0.5)*1.4
    # return math.sqrt(area/(math.pi))
while True:
    import json
    db_conf = {}
    keys = []
    query_conf = {}
    suburbs = {}
    with open('db_conf.json', 'r') as f:
        db_conf = json.load(f)
    couchdb_url = db_conf.get("couchdb_url")
    with open('twitter_api_b_keys.json') as f:
        keys = json.load(f).get("keys")
    with open('Rules.json') as f:
        query_conf = json.load(f)
    with open('areas.json' , 'r') as f:
        suburbs = json.load(f)
    tasks = []
    try:
        for key in query_conf.keys():
                for qs in  query_conf.get(key).keys():
                    tasks.append({"topic":key+"_live","related_to":qs,"q":query_conf.get(key).get(qs)})
        pass
    except BaseException as e:
        print(e)
    suburb_list = suburbs.get("lists")


    count = 0
    geo_ranges = []
    try:
        for task in tasks:

            for suburb in suburb_list:
                print(f"count: {count}")
                try:
                    latitude = suburb["latitude"]
                    longitude = suburb["longitude"]
                    geo_range_local = (str)(latitude)+","+(str)(longitude)+","+(str)(radius(suburb["area"]))+"km"
                    geo_ranges.append(geo_range_local)
                    suburb_x = suburb["suburb"]
                    city = suburb["city"]
                    query = task["q"]
                    big_topic = task["topic"]
                    related = task["related_to"]
                    # print("query :"+query)
                    # print("big_topic : "+big_topic)
                    # print("related : "+related)
                    
                    singlekey_multi_request(keys[count%(len(keys))],query=query,location_range=geo_range_local,db_name=big_topic,suburb=suburb_x,city=city,latitude=latitude,longitude=longitude,related_to=related)
                    count = count + 1
                except BaseException as be:
                    print(be)
                    continue
                    
            pass
    except BaseException as e:
        print(e)
    
    time.sleep(3600*24*6) # Delay for 1 minute (60 seconds).