# -*- coding: utf-8 -*-
# author: shubo

import gzip
import redis
import pymongo
import json
import websocket

class DBConnection:
    def start(self):
        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client['klinedata']
        self.redis = redis.Redis(host="localhost", port=6379, db=0)

    def get_collection(self, name, idunique=True):
        collection = self.db[name]
        if idunique:
            collection.ensure_index('id', unique=True)     
        return collection

    def hset(self, hash, key, value):
        self.redis.hset(hash, key, value)

    def hget(self, hash, key):
        return self.redis.hget(hash,key)
        
    def hexists(self, hash, key):
        return self.redis.hexists(hash,key)

    def rpush(self, list_name, item):
        self.redis.rpush(list_name, item)

    def lpush(self, list_name, item):
        self.redis.lpush(list_name, item)

    def lrange(self, list_name, start, end):
        return self.redis.lrange(list_name, start, end)
    
    def ltrim(self, list_name, start, end):
        self.redis.ltrim(list_name, start, end)

    def llen(self, list_name):
        return self.redis.llen(list_name)

class DataConnection:
    def __init__(self):
        self.ws = websocket.WebSocketApp(
            "wss://api.huobi.br.com/ws",
            on_message = self._on_message,
            on_error = self._on_error,
            on_close = self._on_close,
            on_open = self._on_open)

        self.on_message = None
        self.on_open = None
    
    def start(self):
        #websocket.enableTrace(True)
        self.ws.run_forever()
    
    def stop(self):
        self.ws.close()

    def send(self, msg):
        self.ws.send(msg)

    def _on_open(self, ws):
        print("[DataConnection] opened.")
        if self.on_open != None:
            self.on_open()
        
    def _on_message(self, ws, message):             
        try:
            result = gzip.decompress(message).decode('utf-8')
 
            if result[:7] == '{"ping"':
                ts = result[8:21]
                pong = '{"pong":' + ts + '}'
                ws.send(pong)
            else:
                data = json.loads(result)
                if self.on_message != None:
                    self.on_message(data)     
        except Exception as e:
            print('[on_message] error : ' + str(e))        

    def _on_error(self, ws, error):
        print("[DataConnection] " + str(error))

    def _on_close(self, ws):
        print("[DataConnection] closed.")