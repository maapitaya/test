import csv
from datetime import datetime
import os
import pandas as pd
from ipaddress import ip_interface

N = int(input('Please enter N:'))

TIMEOUT = '-'
OUT_DIR='out/q4/'

SERVER_FAILURE_CSV_FILE_NAME = 'server_failure'
SERVER_FAILURE_CSV_HEADER = ['IP','故障期間from','故障期間to','故障期間']

SUBNET_FAILURE_CSV_FILE_NAME = 'subnet_failure'
SUBNET_FAILURE_CSV_HEADER = ['NetworkAddoress','故障期間from','故障期間to','故障期間']

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
        self.network_address = line[4]

# 行の処理を司るクラス
class LineProccessor:
    def __init__(self):
        self.failure_d = dict()
        self.subnet_d = dict()
        self.server_failure_csv_operator = CsvOperator(SERVER_FAILURE_CSV_FILE_NAME, SERVER_FAILURE_CSV_HEADER)
        self.subnet_failure_csv_operator = CsvOperator(SUBNET_FAILURE_CSV_FILE_NAME, SUBNET_FAILURE_CSV_HEADER)

    # トップレベル行処理
    def line_process(self, l:Line):
        
        if l.network_address in self.subnet_d:
            subnet_group_manager = self.subnet_d[l.network_address]
            subnet_group_manager.add_group_count(l.ip)
            self.subnet_d.update({l.network_address:subnet_group_manager})
        else:
            subnet_group_manager = SubnetGroupManager(l.network_address)
            subnet_group_manager.add_group_count(l.ip)
            self.subnet_d.update({l.network_address:subnet_group_manager})

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
            
            failure_record = FailureRecord(l.ip, start_time, end_time, failure_time)
            
            self.server_failure_csv_operator.add_server_failure_row(failure_record)
            
            # サブネット管理クラスに故障レコードを追加
            subnet_group_manager = self.subnet_d[l.network_address]
            subnet_failure_record = subnet_group_manager.add_failure(failure_record)

            if subnet_failure_record is not None:
                self.subnet_failure_csv_operator.add_subnet_failure_row(subnet_failure_record)
            
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
    
class FailureRecord:
    def __init__(self,ip, start_time, end_time, failure_time):
        self.ip = ip
        self.start_time = start_time
        self.end_time = end_time
        self.failure_time = failure_time

# サブネット内IPグループの故障情報を管理するクラス
class SubnetGroupManager:
    def __init__(self, network_address):
        self.network_address = network_address
        self.server_ips_d = dict() # グループ内で保持するIPを管理
        self.group_max_count_d = dict() # グループ最大数を格納する。
    
    def add_group_count(self, ip):
        self.group_max_count_d.update({ip:''})
        
    # 確定した故障レコードをサブネットグループ内に追加する処理
    def add_failure(self, fr:FailureRecord):
        self.server_ips_d.update({fr.ip:fr})
        
        if len(self.server_ips_d) == len(self.group_max_count_d):
            
            start_time = None
            end_time = None
            
            for f in self.server_ips_d.values():
                if start_time is None:
                    start_time = f.start_time

                if end_time is None:
                    end_time = f.end_time
                    
                if start_time < f.start_time:
                    start_time = f.start_time
                
                if end_time > f.end_time:
                    end_time = f.end_time
                
            if start_time < end_time:
                # サブネット単位で重複あり。サブネット故障レコードを登録用オブジェクトを作成しreturn
                return SubnetFailureRecord(self.network_address, start_time, end_time, end_time - start_time)

class SubnetFailureRecord:
    def __init__(self, network_address, start_time, end_time, failure_time):
        self.network_address = network_address
        self.start_time = start_time
        self.end_time = end_time
        self.failure_time = failure_time

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

    def add_server_failure_row(self, fr:FailureRecord):
        with open(self.out_file_name, 'a', newline='') as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow([fr.ip, fr.start_time, fr.end_time, fr.failure_time])

    def add_subnet_failure_row(self, fr:SubnetFailureRecord):
        with open(self.out_file_name, 'a', newline='') as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow([fr.network_address, fr.start_time, fr.end_time, fr.failure_time])


class Main:
    def __init__(self):
        pass
    
    # IP列に対するネットワークアドレスを値として持つ列を追加
    def get_data(self):
        # メイン処理開始
        df = pd.read_csv('log/q4.log', names=['Time','IP','Ping'])
        df['NetworkAddress'] = df['IP'].apply(self.get_network_address)
        
        return df

    # ネットワークアドレスを取得
    def get_network_address(self, ip):
        return ip_interface(ip).network

# メイン処理
main = Main()
df = main.get_data()

lineProccessor = LineProccessor()

for line in df.itertuples():
    l = Line(line)

    lineProccessor.line_process(l)