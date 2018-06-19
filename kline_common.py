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
    def start(self, use_redis = True, use_db = True):
        if use_db:
            self.client = pymongo.MongoClient('localhost',27017)
            self.db = self.client['klinedata']
            self.db.authenticate('klineapp','klineapp')
        if use_redis:
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

    def publish(self, topic, message):
        self.redis.publish(topic, message)

    def subscribe(self, topic):
        pubsub = self.redis.pubsub()
        pubsub.subscribe(topic)
        return pubsub

class DataConnection:
    
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2

    def __init__(self):
        self.connect_timeout = 30
        self.request_timeout = 5
        self._io_loop = ioloop.IOLoop.instance()
        self._ws_connection = None
        self._connect_status = self.DISCONNECTED
        self._check_alive_on = False

        self.msg_none = None
        self.stop_time = time.time()
        
        
    def start(self, receive_raw=False):
        self.receive_raw = receive_raw
        self._io_loop.add_callback(self.connect)

        if not self._check_alive_on:
            ioloop.PeriodicCallback(self._check_alive, 5000).start()
            self._check_alive_on = True
    
    def connect(self):
        print('[connect] starting...')
        self._connect_status = self.CONNECTING
        headers = httputil.HTTPHeaders({'Content-Type': 'application/json'})
        request = httpclient.HTTPRequest(url = "wss://api.huobi.br.com/ws",
                                         connect_timeout=self.connect_timeout,
                                         request_timeout=self.request_timeout,
                                         headers=headers)
        
        self._ws_connection = tornado.websocket.WebSocketClientConnection(request)
        self._ws_connection.connect_future.add_done_callback(self._on_open)
            
    def _check_alive(self):
        if self._connect_status != self.CONNECTED:
            return

        delta = time.time() - self.alive_time
        if delta > 1:
            print('[connect] timeout.')
            self._connect_status = self.DISCONNECTED
            if self._ws_connection != None:
                self._ws_connection.close()
                self._ws_connection = None
        else:
            if (time.time() - self.update_time ) > 5:
                print('[connect] need update')
                if self.on_need_update != None:
                    self.on_need_update

    def stop(self):
        print('[connect] stop')
        if self._connect_status == self.DISCONNECTED:
            return
        self._connect_status = self.DISCONNECTED
        if self._ws_connection:
            self._io_loop.add_callback(self._reconnect)
            
    def _reconnect(self):
        print('[connect] reconnect')
        
        if self._ws_connection != None:
            self._ws_connection.close()
            self._ws_connection = None
        
        self._io_loop.add_callback(self.connect)

    def send(self, msg):
        if self._connect_status != self.CONNECTED:
            return
        if self._ws_connection == None:
            return

        self._io_loop.add_callback(self._send, msg )

    def _send(self, msg):
        if self._connect_status != self.CONNECTED:
            return
        if self._ws_connection == None:
            return

        self._ws_connection.write_message(msg)

    def _on_open(self, future):
        if future.exception() is None:
            print('[connect] started')
            self._connect_status = self.CONNECTED
            self._ws_connection = future.result()
            
            self.alive_time = time.time() + 5
            self.update_time = time.time()
            
            if self.on_open != None:
                self.on_open()

            self._read_messages()
        else:
            print("[connect] open : " + str(future.exception()))
            self._reconnect()

    def is_connected(self):
        return self._ws_connection is not None

    @gen.coroutine
    def _read_messages(self):
        while self._connect_status == self.CONNECTED and self._ws_connection != None:
            msg = None
            try:
                msg = yield self._ws_connection.read_message()
            except Exception as e:
                msg = None
                print('[read] Failed : ' + str(e))
            
            if msg is None:
                print('[read] None')  
                break

            if self.msg_none != None:
                print('[stop_space] ' + str(time.time() - self.stop_time))
                self.msg_none = None
            self.update_time = time.time()
            self._on_message(msg)

        print('[read] end .')
        self.msg_none = True
        self.stop_time = time.time()
        self._connect_status = self.DISCONNECTED
        self._reconnect()
               
    def _on_message(self, message):
        try:
            result = gzip.decompress(message).decode('utf-8')
 
            if result[:7] == '{"ping"':
                ts = result[8:21]
                pong = '{"pong":' + ts + '}'
                
                self.alive_time = int(ts)/1000
                self.send(pong)
                return
            
            if self.on_message == None:
                return

            if not self.receive_raw:
                data = json.loads(result)
                self.on_message(data)
            else:
                self.on_message(result)

        except Exception as e:
            print('[on_message] error : ' + str(e))        
