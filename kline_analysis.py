import time
import kline_common

# kline分析模块负责根据实时数据以及历史数据分析适合买入跟卖出的点
class Main:
    def __init__(self):
        self.db_conn = kline_common.DBConnection()
               
    def start(self):
        
        self.db_conn.start(True,False)

        pubsub = self.db_conn.subscribe('tick_data')

        for data in pubsub.listen():
            if data['channel'] == b'tick_data':
                print(data['data'])

if __name__ == "__main__":
    main = Main()
    main.start()