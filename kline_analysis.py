# -*- coding: utf-8 -*-
# author: shubo

import time
import json
import logging

import kline_common
import traceback

class DayOpenData:
    def __init__(self):
        self.time = -1
        self.open = -1
        self.value = -1

class DayOpenCache:
    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.day_opens = {}

    def update(self, coin, cur_close):
        cur_time = time.time()
            
        if coin not in self.day_opens:
            day_time = self._get_day_time(cur_time)
            
            collection = self.db_conn.get_collection(coin + '_1day')
            value = collection.find_one({ 'id': day_time})
            if value :
                data = DayOpenData()
                data.time = day_time
                data.open = value['open'] 
                data.value = cur_close            
                self.day_opens[coin] = data
        else:
            data = self.day_opens[coin]
            data.value = cur_close
            
            day_time = self._get_day_time(cur_time)
            
            if day_time > data.open:
                
                collection = self.db_conn.get_collection(coin + '_1day')
                value = collection.find_one({ 'id': day_time})
                
                if value :
                    data.time = day_time
                    data.open = value['open'] 
                else :
                    data.open = -1

    def get_day_open(self, coin):
        if coin in self.day_opens:
            data = self.day_opens[coin]
            return data.open
        return -1

    def get_up_ratio(self):

        count = 0
        up = 0
        for key in self.day_opens:
            item = self.day_opens[key]            
            if item.open != -1 and item.value != -1:
                count += 1
                if item.value > item.open:
                    up += 1
        
        if count < 20:
            return 0
        
        return up / count 
    
    def _get_day_time(self, t):

        t = int(t)
        # 东八区时间戳
        past =  (t + 60 * 60 * 8) % (60 * 60 * 24)
        t = t - past

        return t

class ValueCache:
    def __init__(self, db_conn, coin, period):
        self.db_conn = db_conn
        self.value_cache = []
        self.count = self._create_count(period)
        self.coin = coin
        self.period = period
        self.time_step = self._create_time_step(period)
        self.collection = db_conn.get_collection(coin + '_' + period)

        t = self.db_conn.hget(coin, 'cur_time_' + period)
        self.cur_time = int(t) - 1
        tt = self.cur_time
        for i in range(0,self.count):
            tt -=  self.time_step * i
            v = self.collection.find_one({ 'id': tt})
            if v :
                self.value_cache.append(v)

    def update(self):
        t = int(self.db_conn.hget(self.coin, 'cur_time_' + self.period))
        if t > self.cur_time:
            self.cur_time = t - 1
            v = self.collection.find_one({ 'id': self.cur_time})
            if v:
                self.value_cache.insert(0,v)
                del self.value_cache[-1]

    def get_high_in_past(self):
        high = -1
        for item in self.value_cache:
            if item['high'] > high:
                high = item['high']
        return high

    def _create_time_step(self, period):
        if period == '1min':
            return 60
        elif period == '5min':
            return 60 * 5
        elif period == '15min':
            return 60 * 15
        elif period == '30min':
            return 60 * 30
        elif period == '60min':
            return 60 * 60
        elif period == '1day':
            return 60 * 60 * 24
        elif period == '1week':
            return 60 * 60 * 24 * 7
        else:
            raise Exception('unkonwn period ' + period)

    def _create_count(self, period):
        if period == '1min':
            return 10
        elif period == '5min':
            return 12
        elif period == '15min':
            return 8
        elif period == '30min':
            return 8
        elif period == '60min':
            return 4
        elif period == '1day':
            return 5
        elif period == '1week':
            return 5
        else:
            raise Exception('unkonwn period ' + period)
            
class AllValueCache:
    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.cache_dict = {}

    def update(self, coin, period):
        key = coin + '_' + period
        if not key in self.cache_dict:
            self.cache_dict[key] = ValueCache(self.db_conn, coin, period)
        cache = self.cache_dict[key]
        cache.update()

    def get_high_in_past(self, coin, period):
        key = coin + '_' + period
        if not key in self.cache_dict:
            return -1
        
        cache = self.cache_dict[key]
        return cache.get_high_in_past()


# kline分析模块负责根据实时数据以及历史数据分析适合买入跟卖出的点
class Main:
    def __init__(self):
        self.db_conn = kline_common.DBConnection()
        self.day_open_cache = DayOpenCache(self.db_conn)
        self.all_cache = AllValueCache(self.db_conn)

    def start(self):
        
        self.db_conn.start()
        self._run()

    def _run(self):
        pubsub = self.db_conn.subscribe('tick_data')

        for topic in pubsub.listen():
            if topic['channel'] != b'tick_data':
                continue

            if type(topic['data']) != bytes:
                continue
            try:
                data = json.loads(topic['data'])
                if 'tick' not in data:
                    continue

                self._process_data(data)
                
            except Exception as e:
                #logging.error(str(e))
                msg = traceback.format_exc()
                print(msg)
    
    def _process_data(self, data):
        #print(data)
        splits = data['ch'].split('.')
        coin = splits[1]
        close = data['tick']['close']
        self.day_open_cache.update(coin, close)
        #print(self.day_open_cache.get_up_ratio())
        self.all_cache.update(coin,'1min')
        self.all_cache.update(coin,'60min')
        self.all_cache.update(coin,'1day')
		
        high = self.all_cache.get_high_in_past(coin,'1min')
        high1 = self.all_cache.get_high_in_past(coin,'60min')
        if high1 > high:
           high = high1
        if close < high * 0.75:
           logging.warning('[buy] in up -->' + coin + ':' + str(close) )

        if self.day_open_cache.get_up_ratio() > 0.8:
           day_open = self.day_open_cache.get_day_open(coin)
           if close > day_open*1.015 and close < high * 0.8:
               logging.warning('[buy] in down -->' + coin + ':' + str(close) )



if __name__ == "__main__":
    kline_common.init_logging('kline_analysis')
    main = Main()
    main.start()

