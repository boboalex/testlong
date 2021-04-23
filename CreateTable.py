import json
import sys
import os
import logging
import numpy as np
import pandas as pd
from pyhive import hive
from thrift.transport.TTransport import TTransportException
from HadoopClient import HadoopClient
from pandas.testing import assert_frame_equal
from configparser import ConfigParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
cfg = ConfigParser()
cfg.read('config.ini')


class LoadingData(HadoopClient):
    def __init__(self):
        super().__init__()
        self._create_sqlfile = cfg.get('hadoop_client', 'create_sqlfile')
        self._table_metadata = cfg.get('hadoop_client', 'table_metadata')
        with open(self._table_metadata) as fp:
            self._metadata_dict = json.load(fp)
        # 对元数据字典中的值进行检查
        if None in self._metadata_dict.values():
            raise ValueError('Some values are not defined, you should confirm them first')
        elif None in self._metadata_dict['columns'].values():
            raise ValueError('Some values are not defined, you should confirm them first')
        else:
            self._database_name = self._metadata_dict['database_name']
            self._table_name = self._metadata_dict['table_name']
            self._columns = self._metadata_dict['columns']

    def generate_sql(self, _batch_columns=False, _drop_table=True, **kwargs):
        """

        :param _batch_columns: 批量生成列，以col_num确定列数，默认批量生成string类型的列
        :param _drop_table: 是否在建表前先删除原表
        :param kwargs: 包含：
        1. col_num，在批量生成列时指定列的数量；
        2. datatype，生成列的数据类型
        :return:
        """

        if _batch_columns:
            col_num = kwargs.get('col_num')
            col_length = kwargs.get('col_length')
            if not col_num or not col_length:
                logging.warning('The number or length of columns is not defined, thus it will use default values')
                self._create_batch_columns()

            else:
                self._columns = self._create_batch_columns(col_num, col_length)
        # 生成建表语句
        # columns_str = ""
        # for col_name, col_type in self._columns.items():
        #     columns_str = columns_str + f"`{col_name}`" + "\t" + f"{col_type}" + "," + "\n"
        # # 清除最后一个逗号和换行符
        # columns_str = columns_str.strip(",\n")
        # 使用字符串的join方法可以大大减少代码

        columns_str = ',\n'.join(col_name + '\t' + col_type for col_name, col_type in self._columns.items())
        create_str = f"create table `{self._database_name}.{self._table_name}`(\n " \
                     f"{columns_str} \n" \
                     f") row format delimited fields terminated by ','"
        if _drop_table:
            create_str = f"drop table if exists `{self._database_name}.{self._table_name}`; \n" \
                         + create_str
        # 将建表语句写入sql文件中
        with open(self._create_sqlfile, 'w') as fp:
            fp.write(create_str)

    def _create_batch_columns(self, col_num=10, col_length=100, datatype="string"):
        """
        每列的长度可以设置
        :return:
        """
        base_col = 'kylin' * (col_length // 5) + 'kylin'[0: (col_length % 5)]
        # f"{i:03}"可以实现对字符串格式化处理，字符串一共三位，不够的位数用0补齐，例如001
        col_dict = {base_col + f"{i:03}": datatype for i in range(col_num)}
        return col_dict



    def generate_data(self, col_num, row_num=3):
        country_list = ["CN", "UK", "CA", "RU", "GB", "FR", r"\N"]
        data_array = np.random.choice(country_list, (col_num, row_num), p=[0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.4]).T
        pd.DataFrame(data_array).to_csv(cfg.get('hadoop_client', 'data_file'), header=False, index=False)

    def create_table(self):
        # 以';'为分隔符，分别执行sql，cursor.execute()不能一次执行多条sql
        with open(self._create_sqlfile) as fp:
            create_str_list = fp.read().split(';')
        try:
            with open(self._table_metadata) as fp:
                metadata_dict = json.load(fp)
                database_name = self._database_name
            conn = hive.Connection(host=self._hadoop_host, port=self._hive_port,
                                   username=self._hive_user, database=database_name)
            cursor = conn.cursor()
            for sql in create_str_list:
                cursor.execute(sql)
        except TTransportException as e:
            logging.error('Failed to connect the server, please check the hive configurations')
            sys.exit(1)

    def upload_file(self):
        file_path = os.path.join('/apps/hive/warehouse', f'{self._database_name}.db', self._table_name)
        super().upload_file(file_path, cfg.get('hadoop_client', 'data_file'))

    def check_hive_data(self):
        """
        验证整个造数据过程无问题
        :return:
        """
        conn = hive.Connection(host=self._hadoop_host, port=self._hive_port,
                               username=self._hive_user, database=self._database_name)
        hive_df = pd.read_sql(f"select * from {self._table_name} limit 10", conn)
        # 重新读取时，\N会被解析为None，因此需要使用fillna()方法
        hive_df.fillna(r"\N", inplace=True)
        local_df = pd.read_csv(cfg.get('hadoop_client', 'data_file'),
                               names=[f'{self._table_name}.{col}' for col in self._columns], nrows=10, )
        try:
            assert_frame_equal(hive_df, local_df)
        except AssertionError as e:
            logging.error(e)
            sys.exit(1)


if __name__ == '__main__':
    # for i in range(1, 104):
    #     print(f"{i:03}")
    # generate_sql(_batch_columns=True, _drop_table=True, col_num=100)
    # create_table('testlong.sql')
    # generate_data()

    # delete_file('/apps/hive/warehouse/bo_test.db/xxx/data.csv')

    ld = LoadingData()
    # ld.generate_sql(_batch_columns=True, col_num=4, col_length=13)
    # ld.generate_data(4)
    # ld.create_table()
    # ld.upload_file()
    # ld.check_hive_data()
    ld.generate_sql(True, col_num=100, col_length=5)
    ld.generate_data(100, 4)
    ld.create_table()
    ld.upload_file()