import csv
from datetime import datetime
import os
import pandas as pd

N = int(input('Please enter N:'))
        
TIMEOUT = '-'
OUT_DIR='out/q2/'
SERVER_FAILURE_CSV_FILE_NAME = 'server_failure'
TIME_FORMAT = '%Y%m%d%H%M%S'
# 処理開始日時。ファイルやフォルダの作成に使用
START_TIME = format(datetime.now(), TIME_FORMAT)

# log1行の情報保持クラス
class Line:
    def __init__(self, line):
        self.index = line[0]
        self.time = str(line[1])
        self.ip = line[2]
        self.ping = line[3]

# 行の処理を司るクラス
class LineProccessor:
    def __init__(self):
        self.failure_d = dict()
        self.csvOperator = CsvOperator(SERVER_FAILURE_CSV_FILE_NAME)

    # トップレベル行処理
    def line_process(self, l:Line):
        
         # 行ipが辞書に存在する場合
        if l.ip in self.failure_d:
            self.existing_failure_record(l)
        else:
            self.new_failure_record_start(l)

    # 新規故障レコードの記録開始処理
    def new_failure_record_start(self, l:Line):
        # タイムアウト行であれば辞書に追加
        if l.ping == TIMEOUT:
            failure_dict_value = FailureDictValue(l)
            self.failure_d.update({l.ip:failure_dict_value})
        else: # タイムアウト行でなければ故障開始ではないため、return
            return
    
    # 辞書に故障レコードが存在した際の行処理
    def existing_failure_record(self, l:Line):
        # タイムアウト行であれば故障継続。辞書レコードのタイムアウト連続数をインクリメント
        if l.ping == TIMEOUT:
            failure_dict_value = self.failure_d[l.ip]
            failure_dict_value.add_list(l)
            self.failure_d.update({l.ip:failure_dict_value})
        elif self.failure_d[l.ip].n_num < N: # タイムアウトの連続数がNに満たない場合は故障とみなさないため、辞書を削除
            # 辞書から削除
            del self.failure_d[l.ip]
        else: # タイムアウト行でなければCSVレコード出力
            failure_dict_value = self.failure_d[l.ip]
            first_l = failure_dict_value.get_first_line()

            start_time = datetime.strptime(first_l.time, TIME_FORMAT)
            end_time = datetime.strptime(l.time, TIME_FORMAT)
            failure_time = end_time - start_time
            self.csvOperator.add_row(l.ip, start_time, end_time, failure_time)
            
            # 辞書から削除
            del self.failure_d[l.ip]

class FailureDictValue:
    def __init__(self, l:Line):
        self.n_num = 1
        self.failure_list = []
        self.failure_list.append(l)
        
    def add_list(self, l:Line):
        self.n_num += 1
        self.failure_list.append(l)
    
    def get_first_line(self):
        return self.failure_list[0]
    
    def get_last_line(self):
        return self.failure_list[len(self.failure_list)-1]
    
# CSV処理を司るクラス
class CsvOperator:
    HEADER = ['IP','故障期間from','故障期間to','故障期間']
    
    # 空のCSVファイルを作成
    def __init__(self, file_name):
        self.out_dir = OUT_DIR+START_TIME
        self.out_file_name = self.out_dir+'/'+START_TIME+'_'+file_name+'.csv'
        os.mkdir(self.out_dir)
        with open(self.out_file_name, 'w') as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(self.HEADER)

    def add_row(self, ip, start_time, end_time, failure_time):
        with open(self.out_file_name, 'a', newline='') as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow([ip, start_time, end_time, failure_time])

# メイン処理
df = pd.read_csv('log/q2.log', names=['Time','IP','Ping'])
    
lineProccessor = LineProccessor()

for line in df.itertuples():
    l = Line(line)

    lineProccessor.line_process(l)