import time
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
                print(data['data'])

if __name__ == "__main__":
    main = Main()
    main.start()