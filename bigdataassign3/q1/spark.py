from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import json
import re
from textblob import TextBlob
from elasticsearch import Elasticsearch
from datetime import datetime


TCP_IP = 'localhost'
TCP_PORT = 9006

# Pyspark
# create spark configuration
config = SparkConf()
config.setAppName('TwitterApp')
config.setMaster('local[2]')
config.set("spark.network.timeout","4200s")
config.set("spark.executor.heartbeatInterval","4000s")
# create spark context with the above configuration
spark_con = SparkContext(conf=config)

# create the Streaming Context from spark context with interval size 2 seconds
sparkstreamconf = StreamingContext(spark_con, 4)
sparkstreamconf.checkpoint("checkpoint_TwitterApp")

elastic_search = Elasticsearch([{'host': 'localhost', 'port': 9200}])





def filter_emoji(text_json):
    
    text = text_json['text']
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  
        u"\U0001F300-\U0001F5FF"  
        u"\U0001F680-\U0001F6FF"  
        u"\U0001F1E0-\U0001F1FF"  
                           "]+", flags=re.UNICODE)
    
    text = emoji_pattern.sub(r'', text)
    
    text_json['text'] = text
    
    return(text_json)

def remove_S_Atrate(text_json):
    text = text_json['text']
    
    text = re.sub("@[A-Za-z0-9]+"," ",text)
    text_json['text'] = text
    return(text_json)

def Rem_special_Characs(text_json):
    text = text_json['text']
    text = re.sub("[^0-9A-Za-z \t]"," ",text)
    text_json['text'] = text    
    return(text_json)

def Rem_extra_space(text_json):
    text = text_json['text']
    text = " ".join(re.sub("(\w+:\/\/\S+)"," ",text).split())
    text_json['text'] = text 
    return(text_json)

def get_tweet_polarity(text_json):
    text = text_json['text']
    analysis = TextBlob(text)
    
    sentimental_polarity = analysis.sentiment.polarity
    
    if(sentimental_polarity > 0):
        text_json['sentiment'] = 'positive'
        return(text_json)
    elif(sentimental_polarity == 0):
        text_json['sentiment'] = 'neutral'
        return(text_json)
    else:
        text_json['sentiment'] = 'negative'
        return(text_json)


    

def Load_JSON(x):
    x = json.loads(x)
    return(x)
# read data from port 900
DStream = sparkstreamconf.socketTextStream(TCP_IP, TCP_PORT)
DStream = DStream.map(lambda x:Load_JSON(x))

DStream = DStream.map(lambda x:filter_emoji(x)).map(lambda x:remove_S_Atrate(x)).map(lambda x:Rem_special_Characs(x))
DStream = DStream.map(lambda x:Rem_extra_space(x))
DStream = DStream.map(lambda x:get_tweet_polarity(x))

def Send_To_ElasticSearch(partition):
    
    
    print("send")
    tweets = list(partition)
    print(tweets,len(tweets))
    
    elastic_search = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    
    if(elastic_search.indices.exists(index = "location6")):
        print("if")
        if(len(tweets) != 0):
            for tweet in tweets:
                
                doc = {
                    "text": tweet['text'],
                    "location6": {
                            "lat": tweet['coordinates'][1],
                            "lon": tweet['coordinates'][0]
                            },
                    "sentiment":tweet['sentiment']
                    
                    }
                if(tweet['coordinates'][1] != 0 and tweet['coordinates'][0] !=0 ):
                    elastic_search.index(index="location6", doc_type='request-info', body=doc)
    else:
        print("else")
        mappings = {
        "mappings": {
            "request-info": {
                "properties": {
                    "text": {
                        "type": "text"
                    },
                    "location6": {
                        "type": "geo_point"
                    },
                    "sentiment": {
                        "type": "text"
                    }                        
                }
            }
        }
    }

        elastic_search.indices.create(index='location6', body=mappings)
        if(len(tweets) != 0):
            for tweet in tweets:
                
                doc = {
                    "text": tweet['text'],
                    "location6": {
                            "lat": tweet['coordinates'][1],
                            "lon": tweet['coordinates'][0]
                            },
                    "sentiment":tweet['sentiment']
                    
                    }
                if(tweet['coordinates'][1] != 0 and tweet['coordinates'][0] !=0 ):
                    elastic_search.index(index="location6", doc_type='request-info', body=doc)

       
                    
        
        
#sentimentTweets=dstream_tweets.map(lambda x: {x:getSentiment(x)});

c=DStream.foreachRDD(lambda x: x.foreachPartition(lambda y:Send_To_ElasticSearch(y)))

#################################################

sparkstreamconf.start()
sparkstreamconf.awaitTermination()
