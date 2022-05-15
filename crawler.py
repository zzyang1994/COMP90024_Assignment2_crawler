from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import couchdb
from twarc import Twarc
import json
import osmnx as ox
import subfinder
import re
from textblob import TextBlob
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
nltk.download('punkt')
nltk.download('stopwords')


AREAPATH = "areas.json"
couchDB_server = 'http://admin:password@115.146.95.1:5984/'


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
    t = Twarc(consumer_key, consumer_secret, access_token, access_secret)  # initialize twarc
    addresssearcher = subfinder.subfinder()
    for suburb in suburb_lst:
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
        for tweet in t.filter(track=tr, locations=bounding_box):
            tweets_process = {}
            text = tweet['text']
            result = finder.query_from(text)
            try:
                address = get_address(tweet, addresssearcher)
                tweets_process['suburb'] = suburb
                tweets_process['city'] = "Melbourne"
                tweets_process['coordinates'] = address['coordinates']
            except Exception:
                tweets_process['suburb'] = suburb
                tweets_process['city'] = "Melbourne"
                tweets_process['coordinates'] = None
            sen = sentiment(text)
            tweets_process['sentiments'] = sen[0]
            tweets_process['attitude'] = sen[1]
            tweets_process['text_cleaned'] = sen[2]
            retweet_count = tweet['retweet_count']
            favourite_count = tweet['user']['favourites_count']
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
            except BaseException:
                pass


def add_views_to_db():
    couch = couchdb.Server(couchDB_server)
    db_names = ['transportationxm_live', 'healthxm_live', 'transportationxm_historical', 'healthxm_historical']
    dbs = []
    for db_name in db_names:
        try:
            db = couch.create(db_name)
        except Exception as e:
            print(e)
            pass
        db = couch[db_name]
        dbs.append(db)

    designs = []
    view_names = []

    # function to get suburb names
    map_fun1 = '''function(doc) {
        if (doc.city == 'Melbourne') {
            emit(doc.suburb, 1);
        }
    }
    '''
    reduce_fun1 = "_count"
    designs.append({'views': {
        'get_city': {
            'map': map_fun1,
            'reduce': reduce_fun1
        }
    }})
    view_names.append('_design/suburbs')

    # function to get the tweet counts in suburbs by indicators
    map_fun2 = '''function(doc) {
        if (doc.city == 'Melbourne') {
            emit([doc.suburb, doc.related_to], 1);
        }
    }
    '''
    reduce_fun2 = "_count"
    designs.append({'views': {
        'indicator': {
            'map': map_fun2,
            'reduce': reduce_fun2
        }
    }})
    view_names.append('_design/tweet_count_by_indicator')

    # function to calculate the sentiment index of suburbs by date
    map_fun3 = '''function(doc) {
        emit([doc.suburb, doc.created_at_year, doc.created_at_month, doc.created_at_day], doc.importance);
    }
    '''
    reduce_fun3 = "_sum"
    designs.append({'views': {
        'daily': {
            'map': map_fun3,
            'reduce': reduce_fun3
        }
    }})
    view_names.append('_design/daily_count')

    # function to Word Cloud
    map_fun4 = '''function(doc) {
      var list = doc.text_cleaned.split(" ");
      list.map(v => {
        if (v) {
          emit([doc.suburb, v], 1); 
        }
      })
    }
    '''
    reduce_fun4 = "_count"
    designs.append({'views': {
        'textDetail': {
            'map': map_fun4,
            'reduce': reduce_fun4
        }
    }})
    view_names.append('_design/text')

    # function to get the tweet counts for suburbs by indicators (for bar chart)
    map_fun5 = '''function(doc) {
        emit([doc.suburb, doc.related_to], 1);
    }
    '''
    reduce_fun5 = "_count"
    designs.append({'views': {
        'counts': {
            'map': map_fun5,
            'reduce': reduce_fun5
        }
    }})
    view_names.append('_design/tweet_count')

    # function to map data scenario 1 (scatter)
    map_fun6 = '''function(doc) {
        emit([doc.longitude, doc.latitude], [doc.importance, doc.sentiments, doc.created_at_year, doc.created_at_month, doc.created_at_day]);
    }
    '''
    reduce_fun6 = "_count"
    designs.append({'views': {
        'info': {
            'map': map_fun6,
            'reduce': reduce_fun6
        }
    }})
    view_names.append('_design/tweets')

    # function to map data scenario 2 (geometry)
    map_fun7 = '''function(doc) {
        emit(doc.suburb, [1, doc.importance, doc.sentiments]);
    }
    '''
    reduce_fun7 = "_sum"
    designs.append({'views': {
        'info': {
            'map': map_fun7,
            'reduce': reduce_fun7
        }
    }})
    view_names.append("_design/tweets_geo")

    for db in dbs:
        for i in range(len(designs)):
            try:
                print("Creating view: " + view_names[i])
                print(designs[i])
                print(db)
                db[view_names[i]] = designs[i]
            except:
                print('already exist')
                continue
            sleep(5)


if __name__ == "__main__":
    auth = []
    auth.append("0HJs1AlxVtKrGkhkwWgtM5r6I")# api key
    auth.append("lFKGDdLEWCBlsfq798ODxuS9eBHRORff8kkQEZElNcyONVaJin")# api secret
    auth.append("1519264824890298369-h5a92GpBRgifQNtsTOMQObeBCnznep")# access token
    auth.append("c8OJqjDz6pzkwCVF3fnpkYGhUt9woizJK39BzkeRlGQCE")# access secret

    add_views_to_db()

    print("add views done")

    with open(AREAPATH, 'r') as f:
        print("fetching data...")
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
            print("fetching data2...")
            tmp = key.split(":")
            if tmp[0] == "Query_Transportation":
                track[0] = track[0] + finder.query_dict[key]
            else:
                track[1] = track[1] + finder.query_dict[key]
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                print("fetching data3...")
                for index, tr in enumerate(track):
                    print("fetching data4...")
                    pool.submit(get_data, auth[0], auth[1], auth[2], auth[3], suburb_lst, db_arr[index], finder, tr)
        except BaseException:
            print("cannot start threads")
            raise
