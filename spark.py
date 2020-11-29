from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from geopy.geocoders import Nominatim
from textblob import TextBlob
from elasticsearch import Elasticsearch



TCP_IP = 'localhost'
TCP_PORT = 9001

geolocator = Nominatim(user_agent="HashtagHeatMap")


def processTweet(tweet):

    # Here, you should implement:
    # (i) Sentiment analysis,
    # (ii) Get data corresponding to place where the tweet was generate (using geopy or googlemaps)
    # (iii) Index the data using Elastic Search 


    tweetData = tweet.split("::")
    es=Elasticsearch([{'host':'localhost','port':9200}])

    if len(tweetData) > 1:
        
        text = tweetData[1]
        rawLocation = tweetData[0]

        # (i) Apply Sentiment analysis in "text"
        if float(TextBlob(text).sentiment.polarity) > 0.3:
                stringsentiment = 'Positive'
        elif float(TextBlob(text).sentiment.polarity) < 0.3:
                stringsentiment = 'Negative'
        else:
                stringsentiment = 'Neutral'
        
	# (ii) Get geolocation (state, country, lat, lon, etc...) from rawLocation
        try:
                location = geolocator.geocode(tweetData[0], addressdetails=True)
                lat = location.raw['lat']
                lon = location.raw['lon']
                state = location.raw['address']['state']
                country = location.raw['address']['country']
        except:  
                lat = lon = state = country = None    
        print("\n\n=========================\ntweet: ", tweet)
        print("Raw location from tweet status: ", rawLocation) #[lat,lon,lat,lon]
        #print("lat: ", lat)
        #print("lon: ", lon)
        #print("state: ", state)
        #print("country: ", country)
        print("Text: ", text)
        print("Sentiment: ", stringsentiment)


        # (iii) Post the index on ElasticSearch or log your data in some other way (you are always free!!) 
        if lat != None and lon != None and stringsentiment != None:
	        #dictionary for indexing es
	        esD = {"latitude":lat,"longitude":lon,"State":state,"Country":country,"Sentiment":stringsentiment}
                #index
	        es.index(index = 'tweet-sentiment', doc_type='default', body=esD)



# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)


dataStream.foreachRDD(lambda rdd: rdd.foreach(processTweet))


ssc.start()
ssc.awaitTermination()
