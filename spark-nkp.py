""" 형태소 분석기
    명사 추출 및 빈도수 체크
"""
# konlpy 관련
import sys
from konlpy.tag import Okt
from collections import Counter  # konlpy 를 통해 품사 태깅 후 명사 추출
# tweepy 관련
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

consumer_key = ''
consumer_secret = ''
access_token_key = ''
access_token_secret = ''

def get_tags(text):
    spliter = Okt()
    phrases = spliter.phrases(text)  # text에서 어절 추출
    return phrases


class TweetsListener(StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          raw_tweet = json.loads( data )
          #print(data)
          replaceAll = data.replace("false", "False")
          test = replaceAll.replace("null","None")
          print(test)
          print('-------------------')
          print(raw_tweet['text'])
          print('-------------------')
          tags = get_tags(raw_tweet['text'])
          data = '-'.join(tags)
          print(data)
          self.client_socket.send(data.encode('utf-8'))
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True

  def on_error(self, status):
      print(status)
      return True


def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token_key, access_token_secret)

  twitter_stream = Stream(auth, TweetsListener(c_socket)) # Create a Stream
  twitter_stream.filter(track=['#BTS']) # Starting a Stream


if __name__ == "__main__":
  s = socket.socket()         # Create a socket object
  host = "127.0.0.1"          # Get local machine name
  port = 5556                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print( "Received request from: " + str( addr ) )

  sendData( c )


