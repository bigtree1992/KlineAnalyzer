# -*- coding: utf-8 -*-
# author: shubo

import time
import enum
import random
import queue
import threading
import pymongo
import kline_common
import traceback

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

class KlineTaskConsumer:
    def __init__(self, data_conn, db_conn, task_queue, task_sem):
        self.data_conn = data_conn
        self.db_conn = db_conn
        self.data_conn.on_message = self.on_message
        self.task_queue = task_queue
        self.task_sem = task_sem
        self.current_task = None
        
    def start(self):
        self.thread = threading.Thread(target=self._wait_and_process_task)
        self.running = True
        self.thread.start()

    def stop(self):
        self.running = False

    def _wait_and_process_task(self):
        
        while self.running:
            self.current_task = self.task_queue.get()

            if self.current_task.task_type == TaskType.Stop:
                print('[GetKlineTask] Stop.')
                #self.data_conn.on_message = None
                #self.data_conn.stop()
                self.running = False
                self.task_sem.release()

            elif self.current_task.task_type == TaskType.EndASymbol:
                pass
                #self.db_conn.hset(self.current_task.symbol, 'enabled', 2)
                self.task_sem.release()

            elif self.current_task.task_type == TaskType.GetData:          
                request = self.current_task.get_kline_request()                               
                self.data_conn.send(request)

    def on_message(self, message):
            
        if message['status'] != 'ok':
            print("[GetKlineTask] status != ok -> " + str(message))
            # ToDo : 重新将任务放到队列
            self.task_sem.release()
            return

        db_name = message['id']

        data_count = len(message['data'])            
        if data_count <= 0:
            print("[GetKlineTask] %s Task Stop 1001." % (db_name))             
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
                print("[GetKlineTask] %s DuplicateKeyError." % (db_name))
            except BaseException as e:
                print("[GetKlineTask] InsertOne Error : " + str(e))
        
        end_time = time.time()
        
        print("[insert_all] : " + str(len(message['data'])) + ' time : ' + str(end_time - start_time))
        
        self.task_sem.release()

class KlineTaskProducer:
    def __init__(self, db_conn, init_run=False):
        self.periods = ['1min','5min','15min','30min','60min','1day','1week']
        # self.periods = ['1min']
        self.init_run = init_run
        self.db_conn = db_conn
        self.symbols = None
        self.task_queue = queue.Queue(maxsize = 12)
        self.task_sem = threading.Semaphore(2)

    def start(self):
        self.thread = threading.Thread(target=self.run)
        self.running = True
        self.thread.start()
    
    def stop(self):
        self.running = False

    def run(self):
        if not self.init_run:
            self._run_in_runtime()
        else :
            self._run_by_init()

    def _run_by_init(self):
        self._get_symbols()
        start_time = time.time()
        for symbol in self.symbols:
            enabled = self.db_conn.hget(symbol, 'enabled')
        
            if int(enabled) != 1:
                continue
            
            for period in self.periods:
                self._post_task_by_init(symbol, period)

            task = KlineTask(TaskType.EndASymbol,symbol)
            self._put_task(task)

        task = KlineTask(TaskType.Stop)
        self._put_task(task)

        end_time = time.time()
        print('[init] total_time = ' + str(end_time - start_time))

    def _run_in_runtime(self):
        while self.running:
            start_time = time.time()
            
            self._get_symbols()
            
            cur_minute = int(start_time / 60)

            for symbol in self.symbols:                 
                
                enabled = self.db_conn.hget(symbol, 'enabled')
                if int(enabled) != 2:
                    continue
  
                self._post_task(symbol,'1min')
                
                if cur_minute % 5 == 1:
                    self._post_task(symbol,'5min')

                if cur_minute % 15 == 2:
                    self._post_task(symbol,'15min')

                if cur_minute % 30 == 2:
                    self._post_task(symbol,'30min')

                if cur_minute % 60 == 3:
                    self._post_task(symbol,'60min')

                if cur_minute % (60 * 24) == 3:
                    self._post_task(symbol,'1day')

                if cur_minute % (60 * 24 * 7) == 4:
                    self._post_task(symbol,'1week')

            end_time = time.time()
            #计算代码时间并休息一段时间保证是一分钟运行一次
            total_time = 60 - (end_time - start_time)
            if total_time > 0:
                time.sleep(total_time)

    def _put_task(self, task):
        self.task_sem.acquire()
        self.task_queue.put(task)
    
    def _get_symbols(self):
        symbols = self.db_conn.lrange('symbols', 0, -1)
        self.symbols = []
        for s in symbols:
            self.symbols.append(s.decode('utf-8'))

    def _post_task(self, symbol, period):
        
        time_step = self._create_time_step(period, 1)
        #开始时间设置为当前数据写入到的时间
        start_time = int(self.db_conn.hget(symbol, 'cur_time_' + period))
        end_time = cur_time = int( time.time() )
        #计算拉取的数量，大于300的不处理，需要由init过程单独完成
        if (end_time - start_time) / time_step > 300:
            print('[post_task] warrning : ' + symbol + ' data to get > 300.')
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

class Main:
    def __init__(self):
        self.db_conn = kline_common.DBConnection()
        self.data_conn = kline_common.DataConnection()
        
    def start(self):
        self.db_conn.start()
        self.data_conn.on_open = self.on_open
        self.data_conn.start()

    def on_open(self):
        try:
            producer = KlineTaskProducer(self.db_conn,True)
            producer.start()
            
            consumer = KlineTaskConsumer(self.data_conn,self.db_conn,producer.task_queue,producer.task_sem)
            consumer.start()
            
        except Exception as e:
            msg = traceback.format_exc() # 方式1  
            print (msg)  
    

if __name__ == "__main__":
    main = Main()
    main.start()
    