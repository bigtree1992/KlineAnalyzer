# -*- coding: utf-8 -*-
# author: shubo

import time
import logging
import kline_common

# 交易模块负责进行发送交易指令
class Main:
    def __init__(self):
        self.db_conn = kline_common.DBConnection()
               
    def start(self):
        
        self.db_conn.start(True,False)

        pubsub = self.db_conn.subscribe('trade_cmd')

        for data in pubsub.listen():
            if data['channel'] == b'trade_cmd':
                logging.info(data['data'])

if __name__ == "__main__":
    kline_common.init_logging('kline_trade')
    main = Main()
    main.start()