# -*- coding: utf-8 -*-
# author: shubo

import sys
import time
import enum
import random
import queue
import pymongo
import threading
import traceback
import tornado
import logging

import kline_common

class TaskType(enum.Enum):
    GetData = 1
    EndASymbol = 2
    Stop = 3

class KlineTask:
    def __init__(self, task_type = TaskType.GetData, symbol='', period='1min', start_time=0, end_time=0):
        self.task_type = task_type
        self.symbol = symbol
        self.period = period
        self.start_time = start_time
        self.end_time = end_time

    def get_kline_request(self):
        id = self.symbol + '_' + self.period
        request = """{"req": "market.%s.kline.%s","id": "%s","from": %d,"to": %d}""" \
                % (self.symbol, self.period, id, self.start_time + 1,self.end_time )
        return request

class KlineTaskProducer:
    def __init__(self, db_conn, data_conn, init_run=False):
        self.periods = ['1min','5min','15min','30min','60min','1day','1week']
        # self.periods = ['1min']
        self.init_run = init_run
        self.db_conn = db_conn
        self.symbols = None
        #self.task_queue = queue.Queue(maxsize = 12)            
        self.data_conn = data_conn        
        self.task_max = 2
        self.task_sem = threading.BoundedSemaphore(self.task_max)
        
    def start(self):
        self.thread = threading.Thread(target=self._run)
        self.running = True
        self.thread.start()
    
    def stop(self):
        self.release_all()
        self.running = False
    
    def release_all(self):
        try:
            for x in range(0, self.task_max):
                self.task_sem.release()
        except ValueError as e:
            pass
        
    def _run(self):
        if not self.init_run:
            self._run_in_runtime()
        else :
            self._run_by_init()

    def _run_by_init(self):
        if not self.data_conn.is_connected():
            logging.error('[init] data_conn.is_connected = False')
            return

        self._get_symbols()
        start_time = time.time()
        
        for symbol in self.symbols:
            enabled = self.db_conn.hget(symbol, 'enabled')

            if int(enabled) == 0:
                continue
            
            for period in self.periods:
                self._post_task_by_init(symbol, period)

            if not self.data_conn.is_connected():
                break

            task = KlineTask(TaskType.EndASymbol,symbol)
            self._put_task(task)
            
        task = KlineTask(TaskType.Stop)
        self._put_task(task)

        end_time = time.time()
        logging.info('[init] total_time = ' + str(end_time - start_time))

    def _run_in_runtime(self):
        while self.running:
            if not self.data_conn.is_connected():
                time.sleep(5)
                logging.warning('[running] wait data_conn')
                continue

            start_time = time.time()
            
            self._get_symbols()
            
            cur_minute = int(start_time / 60)

            for symbol in self.symbols:
                
                enabled = self.db_conn.hget(symbol, 'enabled')
                if int(enabled) != 2:
                    continue
                
                if not self.data_conn.is_connected():
                    break

                self._post_task(symbol,'1min')
                
                if cur_minute % 5 == 1:
                    self._post_task(symbol,'5min')

                if cur_minute % 5 == 2:
                    self._post_task(symbol,'15min')

                if cur_minute % 5 == 2:
                    self._post_task(symbol,'30min')

                if cur_minute % 5 == 3:
                    self._post_task(symbol,'60min')

                if cur_minute % 5 == 3:
                    self._post_task(symbol,'1day')

                if cur_minute % 5 == 4:
                    self._post_task(symbol,'1week')

            end_time = time.time()
            #计算代码时间并休息一段时间保证是一分钟运行一次
            total_time = 60 - (end_time - start_time)
            if total_time > 0:
                time.sleep(total_time)

    def _put_task(self, task):
        self.task_sem.acquire()
        self._process_task(task)
    
    def _get_symbols(self):
        symbols = self.db_conn.lrange('symbols', 0, -1)
        self.symbols = []
        for s in symbols:
            self.symbols.append(s.decode('utf-8'))

    def _post_task(self, symbol, period):
        
        #开始时间设置为当前数据写入到的时间
        start_time = int(self.db_conn.hget(symbol, 'cur_time_' + period))
        end_time = cur_time = int( time.time() )

        time_step = self._create_time_step(period, 1)
        
        if (end_time - start_time) < time_step:
            #logging.info('[post_task] no need send task : ' + symbol + ' - ' + period)
            return        
        #计算拉取的数量，大于300的不处理，需要由init过程单独完成
        if (end_time - start_time) / time_step > 300:
            logging.warning('[post_task]' + symbol + ' data to get > 300.')
            return
        
        #生成一个任务并放到任务队列等待处理
        task = KlineTask(TaskType.GetData, symbol, period, start_time, end_time)
        self._put_task(task)
    
    def _post_task_by_init(self, symbol, period):
        
        time_step = self._create_time_step(period, 300)
        
        #开始时间设置为当前数据写入到的时间
        start_time = int(self.db_conn.hget(symbol, 'cur_time_' + period))
        end_time = start_time

        run = True
        count = 0
        while run:

            end_time += time_step
            #如果结束时间大于现在的时间，最终时间要实时更新因为初始化拉取的时间很长
            final_time = int( time.time() ) + time_step
            if end_time > final_time:
                # 往后拉一步确保拉完
                run = False

            #生成一个任务并放到任务队列等待处理
            
            task = KlineTask(TaskType.GetData, symbol, period, start_time, end_time)
            self._put_task(task)

            #继续进行下一个时间段
            start_time += time_step

    def _create_time_step(self, period, data_count):
        if period == '1min':
            return data_count * 60
        elif period == '5min':
            return data_count * 60 * 5
        elif period == '15min':
            return data_count * 60 * 15
        elif period == '30min':
            return data_count * 60 * 30
        elif period == '60min':
            return data_count * 60 * 60
        elif period == '1day':
            return data_count * 60 * 60 * 24
        elif period == '1week':
            return data_count * 60 * 60 * 24 * 7
        else:
            raise Exception('unkonwn period ' + period)

    def _process_task(self, task):

        if task.task_type == TaskType.Stop:
            logging.info('[Task] Stop.')
            self.task_sem.release()
            self.running = False

            self.data_conn.on_message = None
            self.data_conn.stop()

        elif task.task_type == TaskType.EndASymbol:
            self.db_conn.hset(task.symbol, 'enabled', 2)
            self.task_sem.release()

        elif task.task_type == TaskType.GetData:
            request = task.get_kline_request()
            # 如果发送失败 释放任务名额
            
            if not self.data_conn.send(request):
                self.task_sem.release()

    def on_message(self, message):
        
        if message['status'] != 'ok':
            logging.error("[Task] status != ok -> " + str(message))
            self.task_sem.release()
            return

        db_name = message['id']

        data_count = len(message['data'])            
        if data_count <= 0:
            logging.info("[Task] %s Task Stop 1001." % (db_name))             
            self.task_sem.release() 
            return

        datas = message['data']
        # 根据时间进行排序，确保前面的数据总能被插入
        if data_count > 1:
            datas.sort(key = lambda x:x['id'], reverse=False)
        
        start_time = time.time()

        collection = self.db_conn.get_collection(db_name)
        sp = db_name.split('_')

        for single_data in datas: 
            try:
                collection.insert_one(single_data)                                
                self.db_conn.hset(sp[0], 'cur_time_' + sp[1], single_data['id'] + 1)
            except pymongo.errors.DuplicateKeyError as e:
                self.db_conn.hset(sp[0], 'cur_time_' + sp[1], single_data['id'] + 1)
                logging.warning("[Task] insert %s DuplicateKeyError." % (db_name))
            except BaseException as e:
                # ToDo : 可能是数据库掉线了
                logging.error("[Task] %s InsertOne Error : %s" % (db_name, str(e)))
        
        end_time = time.time()
        
        logging.info("[Task] insert %s:%d time : %f" % (db_name,len(message['data']),end_time - start_time))
        
        self.task_sem.release()

class Main:
    def __init__(self, is_init):
        self.is_init = is_init
        self.db_conn = kline_common.DBConnection()
        self.data_conn = kline_common.DataConnection()
        self.data_conn.stop_check_time = 60
        self.data_conn.on_message_stop = self.data_conn.reconnect

        self.first_open = True

    def start(self):       
        try:
            self.db_conn.start()
            self.data_conn.on_open = self.on_open
            self.data_conn.start()

        except Exception as e:
            msg = traceback.format_exc() # 方式1  
            logging.error(msg) 
    
    def stop(self):
        self.producer.stop()
        self.data_conn.stop()
        self.db_conn.start()
    
    def on_open(self):
        try:
            if self.first_open:
                self.first_open = False
                self.producer = KlineTaskProducer(self.db_conn,self.data_conn,self.is_init)
                self.data_conn.on_message = self.producer.on_message
                self.producer.start()
            else:
                self.data_conn.on_message = self.producer.on_message
                self.producer.release_all()
        except Exception as e:
            msg = traceback.format_exc() # 方式1  
            logging.error(msg)

if __name__ == "__main__":
    
    init_run = False

    if len(sys.argv) > 1:
        if sys.argv[1] == 'init':
            init_run = True
    suffix = 'init'
    if not init_run:
        suffix = 'runtime'
    kline_common.init_logging('kline_storager_' + suffix, init_run)#init_run
    
    main = Main(init_run)
    main.start()
    
    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        logging.error('[runtime] exit .')
        main.stop()
