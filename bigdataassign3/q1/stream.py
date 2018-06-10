import json 
import tweepy
import socket
import geocoder

ACCESS_TOKEN = '546225347-SjNtH94vSq8HHKpGpLMWkhy5YhcIlRxCr26Kr4sz'.strip()
ACCESS_SECRET = '3ukk9AN5P8GsldKsziq45jnvZTh2vvPxzWvhbHqkQ244B'.strip()
CONSUMER_KEY = '56Gn6nUgjZE2yUzemzRm7TmUm'.strip()
CONSUMER_SECRET = 'ELGnJfVgosxwuaEYN9kPcBvwmYOEz8KfRWeaewGHUg2LBsvVPj'.strip()

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


hashtag = '#guncontrolnow'

TCP_IP = 'localhost'
TCP_PORT = 9006


# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect((TCP_IP, TCP_PORT))
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()


class MyStreamListener(tweepy.StreamListener):
   
    def on_status(self, status):
#        print(status.text)
#        print(status.user.location)
        loc = str(status.user.location)
        print(loc)
        if( (loc != None) and (loc != "null") ):
            print("check")
            geocode_result = geocoder.google(loc)
            lat_lng = geocode_result.latlng
            print(lat_lng)
            if(  (lat_lng != None) and ( len(lat_lng)!= 0) ):
                lat = lat_lng[0]
                lng = lat_lng[1]
            else:
                lat = 0
                lng = 0
        else:
            print("else")
            lat = 0
            lng = 0
        tweets_loc_coord = {'text':status.text,'location':loc,'coordinates':[lng,lat]}
        str1 = json.dumps(tweets_loc_coord)
#        print(str1)

        print(type(str1))
        
        str1 = str1 + "\n"
        print(str1)
        conn.send(str1.encode("utf-8"))
    
    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())

myStream.filter(track=[hashtag])


