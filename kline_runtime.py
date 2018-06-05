import time
import kline_common

class Main:
    def __init__(self):
        self.db_conn = kline_common.DBConnection()
        self.data_conn = kline_common.DataConnection()
        self.sub_time = 0
        
    def start(self):
        self.db_conn.start(True,False)
        
        self.data_conn.on_open = self.on_open
        self.data_conn.on_message = self.on_message
        self.data_conn.start(True)

    def on_open(self):        
        self.sub_symbols()
    
    def sub_symbols(self):
        
        if time.time() - self.sub_time < 30:
            return;
        self.sub_time = time.time()

        print('[runtime] resub.')
        symbols = self._get_symbols()
        period = '1min'

        for symbol in symbols:            
            request = """{"sub": "market.%s.kline.%s","id": "%s"}""" \
                    % (symbol, period, symbol + '_' + period)

            self.data_conn.send(request)

    def _get_symbols(self):
        symbols = self.db_conn.lrange('symbols', 0, -1)
        symbols_ret = []
        for s in symbols:
            symbols_ret.append(s.decode('utf-8'))
        return symbols_ret

    def on_message(self, msg):
        self.sub_symbols()
        self.db_conn.publish('tick_data', msg)

if __name__ == "__main__":
    main = Main()
    main.start()












