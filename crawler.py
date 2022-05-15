from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import couchdb
from twarc import Twarc
import json
import osmnx as ox
import subfinder
import re
import time
from textblob import TextBlob
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
nltk.download('punkt')
nltk.download('stopwords')


AREAPATH = "areas.json"
couchDB_server = 'http://admin:password@115.146.95.1:5984/'
STOPWORDS = stopwords.words()


class query_finder():
    def __init__(self, filename):
        self.query_dict = {}
        with open(filename, 'r') as f:
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


# find address with coordinates
def get_address(tweets, addresssearcher):
    # find the coordinates
    coordinate = []
    if tweets['coordinates']:
        coordinate = tweets['coordinates']['coordinates']
    else:
        if tweets['place']:
            bounding_box = tweets['place']['bounding_box'][
                'coordinates']
            longitude = (bounding_box[0][0] + bounding_box[1][0] + bounding_box[2][0] + bounding_box[3][0]) / 4
            latitude = (bounding_box[0][1] + bounding_box[1][1] + bounding_box[3][1] + bounding_box[3][1]) / 4
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
    except Exception as e:
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
    text_tokens = word_tokenize(text)  # Removes stop words that have no use in sentiment analysis
    text = [word for word in text_tokens if word not in STOPWORDS]

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


def importance(polarity, popular_index):
    return polarity * popular_index / 10000


# save data to couchDB
def saveData(tweets, tweets_process, db):
    date = datetime.strptime(tweets['created_at'], '%a %b %d %H:%M:%S %z %Y')
    mon = date.strftime("%b")
    imp = importance(tweets_process['sentiments'], tweets_process['favouriteIndex'])
    if tweets_process['coordinates']:
        longitude = tweets_process['coordinates'][0]
        latitude = tweets_process['coordinates'][1]
    else:
        longitude = None
        latitude = None
    db.save({
        '_id': tweets["id_str"],
        'sentiments': tweets_process['sentiments'],
        'attitude': tweets_process['attitude'],
        'longitude': longitude,
        'latitude': latitude,
        'suburb': tweets_process['suburb'],
        'city': tweets_process['city'],
        'importance': imp,
        'truncated': tweets['truncated'],
        'Related to': tweets_process['related_to'],
        'text': tweets['text'],
        'text_cleaned': tweets_process['text_cleaned'],
        'popular_index': tweets_process['favouriteIndex'],
        'created_at_year': date.year,
        'created_at_month': mon,
        'created_at_day': date.day,
    })


def get_data(consumer_key, consumer_secret, access_token, access_secret, suburb_lst, db, finder, tr):
    print("get_data executed")
    try:
        t = Twarc(consumer_key, consumer_secret, access_token, access_secret)  # initialize twarc
        addresssearcher = subfinder.subfinder()
    except Exception as e:
        print("cannot initialize twarc or twarc", e)
        return
    while (True):
        print("start getting data..")
        for index, suburb in enumerate(suburb_lst):
            if index <= 1:
                continue
            geoinfo = ox.geocode_to_gdf(suburb + "," + "melbourne")
            try:
                bounding_box = ""
                bounding_box = bounding_box + str(float(geoinfo.boundary.bounds.minx)) + ","
                bounding_box = bounding_box + str(float(geoinfo.boundary.bounds.miny)) + ","
                bounding_box = bounding_box + str(float(geoinfo.boundary.bounds.maxx)) + ","
                bounding_box = bounding_box + str(float(geoinfo.boundary.bounds.maxy))
            except BaseException as e:
                print("bounding box error! details: ", e)
                raise
            limit = 10000
            print(suburb)
            for tweet in t.filter(track=tr, locations=bounding_box):
                if limit > 0:
                    limit -= 1
                else:
                    break
                # print(tweet["id"], ": ", suburb)
                tweets_process = {}
                try:
                    text = tweet['text']
                    result = finder.query_from(text)
                except Exception as e:
                    print("tweet text error! details: ", e)
                    continue
                try:
                    address = get_address(tweet, addresssearcher)
                    tweets_process['suburb'] = suburb
                    tweets_process['city'] = "Melbourne"
                    tweets_process['coordinates'] = address['coordinates']
                except Exception:
                    tweets_process['suburb'] = suburb
                    tweets_process['city'] = "Melbourne"
                    tweets_process['coordinates'] = None
                try:
                    sen = sentiment(text)
                    tweets_process['sentiments'] = sen[0]
                    tweets_process['attitude'] = sen[1]
                    tweets_process['text_cleaned'] = sen[2]
                    retweet_count = tweet['retweet_count']
                    favourite_count = tweet['user']['favourites_count']
                except Exception as e:
                    print("sen error! details: ", e)
                    continue
                try:
                    popularindex = popularIndex(retweet_count, favourite_count)
                    tweets_process['favouriteIndex'] = popularindex
                except BaseException as e:
                    print("popularindex function not work: ", e)
                if result['match']:
                    tweets_process['related_to'] = result["query"][1]
                else:
                    tweets_process['related_to'] = None
                try:
                    saveData(tweet, tweets_process, db)
                except BaseException as e:
                    pass
            time.sleep(60 * 5)

if __name__ == "__main__":
    auth = [[], []]
    auth[0].append("dFrV3LAkCDbWVFDP23OSTVMBA")  # api key
    auth[0].append("FH5LtaNT1PHEiSnCPDLRfdplkypw4tOQNaFGihaQEZK3vbnwGL")  # api secret
    auth[0].append("1511599549131137028-MHaQtwymceMVFiUQvqmTQZTpNwqq2x")  # access token
    auth[0].append("v8ei56rLcWcld0anXPjb0lxCi4BG2MJKPRLMfBZaxG071")  # access secret
    auth[1].append("0HJs1AlxVtKrGkhkwWgtM5r6I")  # api key
    auth[1].append("lFKGDdLEWCBlsfq798ODxuS9eBHRORff8kkQEZElNcyONVaJin")  # api secret
    auth[1].append("1519264824890298369-h5a92GpBRgifQNtsTOMQObeBCnznep")  # access token
    auth[1].append("c8OJqjDz6pzkwCVF3fnpkYGhUt9woizJK39BzkeRlGQCE")  # access secret

    with open(AREAPATH, 'r') as f:
        areas = json.load(f)
        suburb_lst = list()
        for area in areas["lists"]:
            suburb_lst.append(area["suburb"])
        finder = query_finder("Rules.json")
        couch = couchdb.Server(couchDB_server)
        db_arr = []
        db_arr.append(couch["transportationxm_live"])
        db_arr.append(couch["healthxm_live"])
        track = [[], []]
        for key in finder.query_dict:
            tmp = key.split(":")
            if tmp[0] == "Query_Transportation":
                track[0] = track[0] + finder.query_dict[key]
            else:
                track[1] = track[1] + finder.query_dict[key]
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                for index, tr in enumerate(track):
                    try:
                        pool.submit(get_data, auth[index][0], auth[index][1], auth[index][2], auth[index][3], suburb_lst, db_arr[index], finder, tr)
                        print("Thread " + str(index) + " started")
                    except Exception as e:
                        print(e)
                        continue
        except BaseException:
            print("cannot start threads")
            raise
