import time
import json

import kline_common

# kline分析模块负责根据实时数据以及历史数据分析适合买入跟卖出的点
class Main:
    def __init__(self):
        self.db_conn = kline_common.DBConnection()
               
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
                print("[error] : " + str(e))
                print(topic['data'])
    
    def _process_data(self, data):
        print(data['ch'] , data['tick']['id'], data['tick']['close'])


if __name__ == "__main__":
    main = Main()
    main.start()