# -*- coding: utf-8 -*-
# author: shubo

import gzip
import redis
import pymongo
import json

from tornado import gen
from tornado import httpclient
from tornado import httputil
from tornado import ioloop
import tornado
import tornado.websocket

import threading
import time


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
    
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2

    def __init__(self):        
        self.connect_timeout = 30
        self.request_timeout = 30
        self._io_loop = ioloop.IOLoop.instance()
        self._ws_connection = None
        self._connect_status = self.DISCONNECTED
        

    def start(self):
        self._connect_status = self.CONNECTING
        headers = httputil.HTTPHeaders({'Content-Type': 'application/json'})
        request = httpclient.HTTPRequest(url = "wss://api.huobi.br.com/ws",
                                         connect_timeout=self.connect_timeout,
                                         request_timeout=self.request_timeout,
                                         headers=headers)
        
        ws_conn = tornado.websocket.WebSocketClientConnection(request)
        ws_conn.connect_future.add_done_callback(self._on_open)
        
        try:
            self._io_loop.start()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):

        if self._connect_status != self.DISCONNECTED:
            self._connect_status = self.DISCONNECTED
            self._ws_connection and self._ws_connection.close()
            self._ws_connection = None
        
    def send(self, msg):
        if self._ws_connection:
            self._io_loop.add_callback(self._send, msg )

    def _send(self, msg):            
        self._ws_connection.write_message(msg)

    def _on_open(self, future):
        if future.exception() is None:
            self._connect_status = self.CONNECTED
            self._ws_connection = future.result()
            
            if self.on_open != None:
                self.on_open()

            self._read_messages()
        else:
            print("[_on_open] " + str(future.exception()))
            self.stop()

    def is_connected(self):
        return self._ws_connection is not None

    @gen.coroutine
    def _read_messages(self):
        while True:
            msg = yield self._ws_connection.read_message()
            if msg is None:
                self.stop()
                break

            self._on_message(msg)
        
        
    def _on_message(self, message):             
        try:
            result = gzip.decompress(message).decode('utf-8')
 
            if result[:7] == '{"ping"':
                ts = result[8:21]
                pong = '{"pong":' + ts + '}'
                self.send(pong)
            else:
                data = json.loads(result)
                if self.on_message != None:
                    self.on_message(data)
                    print("[message] end")
        except Exception as e:
            print('[on_message] error : ' + str(e))        
