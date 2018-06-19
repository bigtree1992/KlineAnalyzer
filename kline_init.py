# -*- coding: utf-8 -*-
# author: shubo

import time
import redis
import logging
import pymongo

import kline_common

# 用于初始化交易对信息以及，交易对的kline初始时间
# 支持添加交易对之后动态更新，支持脚本的反复多次运行，不会出错
class Main:
	def __init__(self):
		self.db_conn = kline_common.DBConnection()
		
	def start(self,overwrite):
		self.db_conn.start()
		# 需要进行初始化的交易对
		tradesymbols = [ {'symbol':'btcusdt','init_time':'2017-10-26 00:00:00'}, 
					   {'symbol':'bchusdt','init_time':'2017-10-31 00:00:00'},
					   {'symbol':'ethusdt','init_time':'2017-10-26 00:00:00'},
					   {'symbol':'etcusdt','init_time':'2017-10-31 00:00:00'},
					   {'symbol':'ltcusdt','init_time':'2017-10-27 00:00:00'},
					   {'symbol':'eosusdt','init_time':'2017-12-05 00:00:00'},
					   {'symbol':'xrpusdt','init_time':'2017-11-24 00:00:00'},
					   {'symbol':'omgusdt','init_time':'2017-12-05 00:00:00'},
					   {'symbol':'dashusdt','init_time':'2017-11-10 00:00:00'},
					   {'symbol':'zecusdt','init_time':'2017-12-07 00:00:00'},
					   {'symbol':'iotausdt','init_time':'2018-05-08 00:00:00'},
					   {'symbol':'adausdt','init_time':'2018-04-17 00:00:00'},
					   {'symbol':'steemusdt','init_time':'2018-04-28 00:00:00'},
					   {'symbol':'wiccusdt','init_time':'2018-05-31 00:00:00'}, 
					   {'symbol':'socusdt','init_time':'2018-05-11 00:00:00'}, 
					   {'symbol':'ctxcusdt','init_time':'2018-04-24 00:00:00'},
					   {'symbol':'actusdt','init_time':'2018-04-23 00:00:00'}, 
					   {'symbol':'btmusdt','init_time':'2018-04-18 00:00:00'},
					   {'symbol':'btsusdt','init_time':'2018-04-15 00:00:00'},
					   {'symbol':'ontusdt','init_time':'2018-04-09 00:00:00'}, 
					   {'symbol':'iostusdt','init_time':'2018-01-23 00:00:00'}, 
					   {'symbol':'htusdt','init_time':'2018-02-01 00:00:00'},
					   {'symbol':'trxusdt','init_time':'2018-03-09 00:00:00'}, 
					   {'symbol':'dtausdt','init_time':'2018-02-03 00:00:00'},
					   {'symbol':'neousdt','init_time':'2018-01-04 00:00:00'},
					   {'symbol':'qtumusdt','init_time':'2017-12-12 00:00:00'},
					   {'symbol':'smtusdt','init_time':'2018-01-24 00:00:00'},
					   {'symbol':'elausdt','init_time':'2018-03-05 00:00:00'},
					   {'symbol':'venusdt','init_time':'2018-01-22 00:00:00'}, 
					   {'symbol':'thetausdt','init_time':'2018-01-31 00:00:00'},
					   {'symbol':'sntusdt','init_time':'2018-01-05 00:00:00'},
					   {'symbol':'zilusdt','init_time':'2018-02-08 00:00:00'}, 
					   {'symbol':'xemusdt','init_time':'2018-01-26 00:00:00'}, 
					   {'symbol':'nasusdt','init_time':'2018-02-12 00:00:00'},
					   {'symbol':'ruffusdt','init_time':'2018-02-09 00:00:00'},
					   {'symbol':'hsrusdt','init_time':'2017-12-22 00:00:00'},
					   {'symbol':'letusdt','init_time':'2018-02-02 00:00:00'},
					   {'symbol':'mdsusdt','init_time':'2018-03-08 00:00:00'},
					   {'symbol':'storjusdt','init_time':'2018-01-09 00:00:00'},
					   {'symbol':'elfusdt','init_time':'2018-01-25 00:00:00'},
					   {'symbol':'itcusdt','init_time':'2018-02-22 00:00:00'}, 
					   {'symbol':'cvcusdt','init_time':'2018-01-11 00:00:00'}, 
					   {'symbol':'gntusdt','init_time':'2018-01-08 00:00:00'}]
		  
		#将数据设置相关的redis数据
		self.db_conn.ltrim('symbols',-1, 0)
		for item in tradesymbols:
			try:
				symbol = item['symbol']
				self.db_conn.rpush('symbols',str(symbol))
			except Exception as e:
				logging.error("[init] 1 " + str(e))
		
		cur_times = ['cur_time_1min', 'cur_time_5min', 
					 'cur_time_15min', 'cur_time_30min', 'cur_time_60min', 
					 'cur_time_1day', 'cur_time_1week']
				
		for item in tradesymbols:
			try:
				symbol = item['symbol']
				init_time = item['init_time']
				#设置这个交易对的Kline初始时间
				self.db_conn.hset(symbol,'init_time',init_time)
				#如果当前Kline时间不存在就设置这个交易对的当前Kline时间为初始时间                
				for cur_time_key in cur_times:
					if not self.db_conn.hexists(symbol,cur_time_key) or overwrite:
						time_t = time.strptime(init_time, "%Y-%m-%d %H:%M:%S")
						cur_time = int(time.mktime(time_t))
						self.db_conn.hset(symbol,cur_time_key, cur_time)
				
				#添加一个开关方便动态进行数据获取
				if overwrite:
					 self.db_conn.hset(symbol,'enabled',1)
				elif not self.db_conn.hexists(symbol,'enabled'):
					self.db_conn.hset(symbol,'enabled',0)

			except Exception as e:
				logging.error("[init] 2 " + str(e))

if __name__ == "__main__":
	init_logging('kline_init', True)
	main = Main()
	main.start(False)
