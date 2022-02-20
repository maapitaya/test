import csv
from datetime import datetime
import os
import pandas as pd

N = int(input('Please enter N:'))
M = int(input('Please enter M:'))
T = int(input('Please enter T:'))

ORIGINAL_TIMEOUT = '-'
TIMEOUT = 4000
OUT_DIR='out/q3/'
SERVER_FAILURE_FILE_NAME = 'server_failure'
SERVER_FAILURE_HEADER = ['IP','故障期間from','故障期間to','故障期間']
SERVER_OVERLOAD_FILE_NAME = 'server_over_load'
SERVER_OVERLOAD_HEADER = ['IP','過負荷期間from','過負荷期間to','過負荷期間','過負荷内訳']

TIME_FORMAT = '%Y%m%d%H%M%S'
# 処理開始日時。ファイルやフォルダの作成に使用
START_TIME = format(datetime.now(), TIME_FORMAT)

failure_d = dict()

# log1行の情報保持クラス
class Line:
    def __init__(self, line):
        self.index = line[0]
        self.time = str(line[1])
        self.ip = line[2]
        self.ping = line[3]
        self.average = line[4]

# 行の処理を司るクラス
class LineProccessor:

    def __init__(self):
        self.failure_d = dict() #故障期間登録のための行データ格納辞書
        self.overload_d = dict()
        self.failure_csvOperator = CsvOperator(SERVER_FAILURE_FILE_NAME, SERVER_FAILURE_HEADER)
        self.overload_csvOperator = CsvOperator(SERVER_OVERLOAD_FILE_NAME, SERVER_OVERLOAD_HEADER)
    
    # トップレベル行処理
    def line_process(self, l:Line):
                
        self.failure_process(l)
        self.overload_process(l)
    
    def overload_process(self, l:Line):

         # 行ipが辞書に存在する場合
        if l.ip in self.overload_d:
            self.existing_overload_record(l)
        else:
            self.new_overload_record_start(l)

    # 新規過負荷レコードの記録開始処理
    def new_overload_record_start(self, l:Line):
        # 平均応答時間がTを超えている→辞書に追加
        if l.average > T:
            overload_dict_value = OverloadDictValue(l)
            self.overload_d.update({l.ip:overload_dict_value})
        else: # 過負荷ではないため、return
            return
    
    # 辞書に過負荷レコードが存在した際の行処理
    def existing_overload_record(self, l:Line):
        # 過負荷であれば記録継続。辞書レコードの過負荷終了期間を更新
        if l.average > T:
            overload_dict_value = self.overload_d[l.ip]
            overload_dict_value.add_list(l)
            self.overload_d.update({l.ip:overload_dict_value})

        else: # 過負荷が明けたらCSVに出力
            overload_dict_value = self.overload_d[l.ip]
            first_l = overload_dict_value.get_first_line()
            start_time = datetime.strptime(first_l.time, TIME_FORMAT)
            end_time = datetime.strptime(l.time, TIME_FORMAT)
            overload_time = end_time - start_time
            
            # 過負荷内訳を作成
            overload_breakdown = ''
            num = 0
            for l in overload_dict_value.overload_list:
                if num != 0:
                    overload_breakdown += ', '
                overload_breakdown += str(l.average)
                num +=1
                
            self.overload_csvOperator.add_overload_row(l.ip, start_time, end_time, overload_time, overload_breakdown)

            # 辞書から削除
            del self.overload_d[l.ip]

    # 故障期間CSV作成処理
    def failure_process(self, l:Line):

         # 行ipが辞書に存在する場合
        if l.ip in self.failure_d:
            self.existing_failure_record(l)
        else:
            self.new_failure_record_start(l)

    # 新規故障レコードの記録開始処理
    def new_failure_record_start(self, l:Line):
        # タイムアウト行であれば辞書に追加。
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

        else: # CSVに出力
            failure_dict_value = self.failure_d[l.ip]
            first_l = failure_dict_value.get_first_line()

            start_time = datetime.strptime(first_l.time, TIME_FORMAT)
            end_time = datetime.strptime(l.time, TIME_FORMAT)
            failure_time = end_time - start_time
            self.failure_csvOperator.add_failure_row(l.ip, start_time, end_time, failure_time)
            
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

class OverloadDictValue:
    def __init__(self, l:Line):
        self.n_num = 1
        self.overload_list = []
        self.overload_list.append(l)
        
    def add_list(self, l:Line):
        self.n_num += 1
        self.overload_list.append(l)

    def get_first_line(self):
        return self.overload_list[0]
    
    def get_last_line(self):
        return self.overload_list[len(self.overload_list)-1]

# CSV処理を司るクラス
class CsvOperator:
    
    # 空のCSVファイルを作成
    def __init__(self, file_name, header):
        self.out_dir = OUT_DIR+START_TIME
        self.out_file_name = self.out_dir+'/'+START_TIME+'_'+file_name+'.csv'
        os.makedirs(self.out_dir, exist_ok=True)
        
        with open(self.out_file_name, 'w') as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(header)

    # サーバ故障CSVの行追記メソッド
    def add_failure_row(self, ip, start_time, end_time, period):
        with open(self.out_file_name, 'a', newline='') as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow([ip, start_time, end_time, period])

    # サーバ過負荷CSVの行追記メソッド
    def add_overload_row(self, ip, start_time, end_time, period, overload_breakdown):
        with open(self.out_file_name, 'a', newline='') as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow([ip, start_time, end_time, period, overload_breakdown])

# メイン処理開始
df = pd.read_csv('log/q3.log', names=['Time','IP','Ping'])
# タイムアウト行を含んだ平均応答時間算出のため、タイムアウトを4000ミリ秒として扱うため、置換
df['Ping'] = df['Ping'].replace(ORIGINAL_TIMEOUT, TIMEOUT)

# 直近M回の平均応答時間を[Average]として列追加
df['Average'] = df.groupby('IP',group_keys=False)['Ping'].rolling(window=M).mean().reset_index(0,drop=True)

df_r=df.reset_index(drop=True).reset_index()

lineProccessor = LineProccessor()

for line in df.itertuples():

    l = Line(line)

    lineProccessor.line_process(l)
