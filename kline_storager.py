# -*- coding: utf-8 -*-
# author: shubo

import time
import random
import queue
import threading
import pymongo
import kline_common

class KlineTask:
	def __init__(self, symbol, period, start_time, end_time, collection):
		self.symbol = symbol
		self.period = period
		self.start_time = start_time
		self.end_time = end_time
		self.collection = collection

	def get_kline_request(self):
		id =  'id-' + self.symbol + '-' + self.period
		request = """{"req": "market.%s.kline.%s","id": "%s","from": %d,"to": %d}""" \
				% (self.symbol, self.period, id, self.start_time + 1,self.end_time )
		return request

class KlineTaskConsumer:
	def __init__(self, data_conn, db_conn, task_queue):
		self.data_conn = data_conn
		self.db_conn = db_conn
		self.data_conn.on_message = self.on_message
		self.task_queue = task_queue
		self.current_task = None
		
	def start(self):
		self._wait_and_process_task()

	def _wait_and_process_task(self):
		self.current_task = None
		self.current_task = self.task_queue.get()
		request = self.current_task.get_kline_request()
		print(request)
		self.data_conn.send(request)

	def on_message(self, message):
		data_count = len(message['data'])
		if data_count <= 0:
			print("[GetKlineTask] %s Task Stop 1001." % (self.current_task.symbol))
			self._wait_and_process_task()
			return

		datas = message['data']
		# 根据时间进行排序，确保最新的数据总能被插入
		if data_count > 1:
			datas.sort(key = lambda x:x['id'], reverse=True)

		print("before insert_all : " + str(len(message['data'])))
		for single_data in datas: 
			try:
				self.current_task.collection.insert_one(single_data)
				self.db_conn.hset(self.current_task.symbol, 'cur_time_' + self.current_task.period, single_data['id'] + 1)
			except pymongo.errors.DuplicateKeyError as e:
				print("[GetKlineTask] %s Task Stop 1002." % (self.current_task.symbol))
				self._wait_and_process_task()
				return
			except BaseException as e:
				print("[GetKlineTask] InsertOne Error : " + str(e))
				self._wait_and_process_task()
				return

		self._wait_and_process_task()

class KlineTaskProducer:
	def __init__(self, db_conn, init_run=False):
		self.periods = ['1min','5min','15min','30min','60min','1day','1week']
		self.init_run = init_run
		self.db_conn = db_conn
		self.symbols = None
		self.task_queue = queue.Queue(maxsize = 2000)
		threading.Thread.__init__(self, name='KlineTaskProducer')

	def start(self):
		self.thread = threading.Thread(target=self.run)
		self.running = True
		self.thread.start()
	
	def stop(self):
		self.running = False

	def run(self):
		if not self.init_run:
			while self.running:
				self._get_symbols()
				cur_time = int( time.time() ) 
				self.post_task('1min')

				time.sleep(60)
		else :
			self._get_symbols()
			self.post_task_by_init('1min')
			#post_task_by_init('5min')
			#post_task_by_init('15min')
			#post_task_by_init('30min')
			#post_task_by_init('60min')
			#post_task_by_init('1day')
			#post_task_by_init('1week')
	
	def _get_symbols(self):
		symbols = self.db_conn.lrange('symbols', 0, -1)
		self.symbols = []
		for s in symbols:
			self.symbols.append(s.decode('utf-8'))

	def post_task(self, period):

		for symbol in self.symbols:            
			
			enabled = self.db_conn.hget(symbol, 'enabled')
			if int(enabled) == 0:
				continue

			time_step = self._create_time_step(period, 1)
			#开始时间设置为当前数据写入到的时间
			start_time = self.db_conn.hget(symbol, 'cur_time_' + period)
			collection = self.db_conn.get_collection(symbol + '-' + period)

			end_time = start_time + time_step
			#生成一个任务并放到任务队列等待处理
			task = KlineTask(symbol, period, start_time, end_time, collection)
			self.task_queue.put(task)
			
	def post_task_by_init(self, period):
		
		for symbol in self.symbols:

			enabled = self.db_conn.hget(symbol, 'enabled')
			if int(enabled) == 0:
				continue

			collection = self.db_conn.get_collection(symbol + '-' + period)
			
			time_step = self._create_time_step(period, 300)
			
			#开始时间设置为当前数据写入到的时间
			start_time = int(self.db_conn.hget(symbol, 'cur_time_' + period))
			end_time = start_time 
						
			run = True
			while run:                
				end_time += time_step
				#如果结束时间大于现在的时间，最终时间要实时更新因为初始化拉取的时间很长
				final_time = int( time.time() ) + time_step
				if end_time > final_time:
					# 往后拉一步确保拉完
					run = False

				#生成一个任务并放到任务队列等待处理
				task = KlineTask(symbol, period, start_time, end_time, collection)
				self.task_queue.put(task)

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
		producer = KlineTaskProducer(self.db_conn,True)
		producer.start()
		consumer = KlineTaskConsumer(self.data_conn,self.db_conn,producer.task_queue)
		consumer.start()

if __name__ == "__main__":
	main = Main()
	main.start()
	