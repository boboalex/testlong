from requests import HTTPError
from configparser import ConfigParser
from basic import BasicHttpClient
import json
import logging
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
cfg = ConfigParser()
cfg.read('config.ini')

class KIClient(BasicHttpClient):
    def __init__(self):
        super().__init__()
        self._ki_create_dataset = cfg.get('project', 'create_dataset_name')
        self._ki_update_dataset = cfg.get('project', 'update_dataset_name')
        self._headers = {'Accept': '*/*',
                         'Accept-Language': 'en',
                         'Content-Type': 'application/json;charset=utf-8'
                         }

        self._ki_base_url = f"http://{self._ki_host}:{self._ki_port}"

    def _request(self, method, url, **kwargs):  # pylint: disable=arguments-differ
        return super()._request(method, self._ki_base_url + url, **kwargs)

    def list_projects(self):
        return requests.request('GET', 'http://10.1.2.95:9002/kylin/projects', headers = self._headers, auth=('BO', 'kylin@2020'))

    def datasets_desc(self, page_num=0, page_size=1000):
        payload = {
            'projectName': self._ki_project,
            'pageNum': page_num,
            'pageSize': page_size
        }
        resp = self._request('GET', '/kylin/dataset', params=payload)
        return resp

    # def get_dataset_id(self, dataset_name):
    # 方法一：以数据集的id作为字典的key，然后遍历字典的值，如果存在这个数据集的名称就返回数据集id，否则返回None
    #     dataset_list = self.datasets_desc()['data']['list']
    #     ids_names_dict = {dataset['id']: dataset['dataset'] for dataset in dataset_list}
    #     print(ids_names_dict)
    #     for dataset in dataset_list['data']['list']:
    #         if dataset['dataset'] == dataset_name:
    #             return dataset_id
    #     print(f'The dataset {dataset_name} not exists')
    #     return None
    def get_dataset_id(self, dataset):
        """
        方法二：以数据集的名称作为字典的key，以数据集的id作为字典的value，如果存在这个数据集的名称就返回数据集id，否则抛出异常
        由于同一项目下，数据集名称无法重复，因此该方法具备可行性
        :param dataset_name:
        :return:
        """
        dataset_list = self.datasets_desc()['data']['list']
        # 后置单下划线可以避免与关键词重复
        ids_names_dict = {dataset_['dataset']: dataset_['id'] for dataset_ in dataset_list}
        return ids_names_dict.get(dataset)

    def delete_dataset(self, dataset):
        dataset_id = self.get_dataset_id(dataset)
        if dataset_id is None:
            logging.info(f'The dataset {dataset} not exists, so it can not be deleted')
        else:
            try:
                self._request('DELETE', f'/kylin/dataset/{dataset_id}')
                logging.info(f'delete dataset: "{dataset}" successfully')
            except HTTPError:
                logging.error(f'delete dataset: "{dataset}" failed')

    def create_dataset(self):
        """
        1. 读取json文件以获得payload，需要注意的是，json文件中True和False应该写为true/false，null也改为""，json文件不支持
        单引号，因此如果出现引号包裹引号的情况，里面的引号也要用双引号，并加上转义符\
        2. 在准备新建数据集之前，需要注意json文件中的project和dataset名称需要和config.ini配置文件中一致
        :return:
        """
        with open('create_dataset_payload.json') as fp:
            payload = json.load(fp)
        try:
            self._request('POST', '/kylin/dataset', json=payload)
            logging.info(f'create dataset: "{self._ki_create_dataset}" successfully')
        except HTTPError:
            logging.error(f'create dataset: "{self._ki_create_dataset}" failed')

    def renew_dataset(self):
        dataset_id = self.get_dataset_id()
        if dataset_id is None:
            self.create_dataset()
        else:
            try:
                self._request('DELETE', f'/kylin/dataset/{dataset_id}')
                logging.info(
                    f'delete the old dataset: "{self._ki_create_dataset}" successfully, then it will be renewed')
                self.create_dataset()
            except HTTPError:
                logging.error(f'delete dataset: "{self._ki_create_dataset}" failed, so it cannot be renewed')

    def _set_large_description(self, payload):
        with open('large_description.txt', 'r', encoding='utf-8') as fp:
            large_description = fp.read()
        for table in payload['models'][0]['dimension_tables']:
            for col in table['dim_cols']:
                col['desc'] = large_description
        return payload

    def update_dataset(self, large_description=False):
        """
        在准备更新数据集之前，需要注意json文件中的project和dataset名称需要和config.ini配置文件中一致
        :param large_description:
        :return:
        """
        with open('testlong.json') as fp:
            payload = json.load(fp)
        if payload['project'] != self._ki_project or payload['dataset_name'] != self._ki_update_dataset:
            raise ValueError('The project or dataset in json is not same as the configuration, please check it first')
        if large_description:
            payload = self._set_large_description(payload)
        dataset_id = self.get_dataset_id(self._ki_update_dataset)
        if dataset_id is None:
            logging.info(f'The dataset {self._ki_update_dataset} not exists, so it will not be updated')
        else:
            try:
                self._request('PUT', f'/kylin/dataset/{dataset_id}', json=payload)
                logging.info(f'update dataset: "{self._ki_update_dataset}" successfully')
            except HTTPError:
                logging.error(f'update dataset: "{self._ki_update_dataset}" failed')


if __name__ == '__main__':
    client = KIClient()
    # client.create_dataset()
    resp = client.list_projects()
    print(resp.status_code)