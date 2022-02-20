#テストデータの生成プログラム
import csv
from datetime import datetime, timedelta
import random

class DataMaker:
    header = ['Date', 'IPAddress', 'ping']

    def __init__(self, base_time):
        self.base_time = base_time

    def result(self):
        return_time = random.randint(0, 5000)
        if return_time > 4000:
            return '-'
        else:
            
            r = random.randint(0, 50)
            if r < 2 :
                return return_time
            else:
                return return_time % 15 + 1
    
    def getTime(self):
        self.base_time += timedelta(seconds=random.randint(0, 5))

    def make(self):
        
        with open('ping.csv', 'w') as f:
            w = csv.writer(f, lineterminator="\n")
            
            for host in hosts:
                
                # 初登場の場合、時間の取得base_time
                self.getTime()
                time = self.base_time

                num = 0
                while num < 50:
                    if num != 0:
                        time += timedelta(minutes=1)
                    
                    w.writerow([format(time, '%Y%m%d%H%M%S'), host, self.result()])
                    num += 1
        f.close()
        
        
hosts = [
    '192.168.1.1/24',
    '192.168.1.11/24',
    '192.168.1.22/24',
    '192.168.1.33/24',
    '192.168.10.3/24',
    '192.168.100.10/24',
    '192.168.100.110/24',
    '192.168.100.11/24',
    '192.168.100.111/24',
    '192.168.100.2/24',
    '10.20.30.1/16',
    '10.20.30.2/16',
    '172.16.0.6/16',
    '172.16.10.8/16',
    '172.16.10.1/16',
    '172.16.10.2/16'
]

# 基礎の時間を生成
base_time = datetime.now()

d = DataMaker(base_time)

d.make()

