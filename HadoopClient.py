from hdfs.client import InsecureClient
from configparser import ConfigParser
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
cfg = ConfigParser()
cfg.read('config.ini')


class HadoopClient:
    def __init__(self):
        self._hadoop_host = cfg.get('hadoop_client', 'hadoop_host')
        self._hive_port = cfg.get('hadoop_client', 'hive_port')
        self._hive_user = cfg.get('hadoop_client', 'hive_user')
        self._hdfs_port = cfg.get('hadoop_client', 'hdfs_port')
        self._hdfs_url = f"http://{self._hadoop_host}:{self._hdfs_port}"

    def upload_file(self, filepath, file):
        try:
            client = InsecureClient(self._hdfs_url, user=self._hive_user)
            client.upload(filepath, file)
        #
        except Exception as e:
            pass

    def list_files(self, filepath):
        try:
            client = InsecureClient(self._hdfs_url, user=self._hive_user)
            file_list = client.list(filepath)
            return file_list
        except Exception as e:
            pass

    def delete_file(self, file):
        try:
            client = InsecureClient(self._hdfs_url, user=self._hive_user)
            client.delete(file)
        except Exception as e:
            print(e)

if __name__ == '__main__':
    client = HadoopClient()
    client.list_files('/apps/hive/warehouse/bo_test.d')