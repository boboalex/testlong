import requests
from requests import HTTPError
import logging
payload = {"project": "bo_test_project",
                   "dataset_name": "数据集测试",
                   "description": "",
                   "type": "SQL",
                   "front_v": "v0.2",
                   "model_relations": [{"model_left": "botestdataset",
                                        "model_right": "",
                                        "relation": []}],
                   "models": [{"model_name": "botestdataset",
                               "model_alias": "botestdataset",
                               "fact_table": "SSB_SF1.P_LINEORDER",
                               "dimension_tables": [{"name": "P_LINEORDER",
                                                     "type": "regular",
                                                     "alias": "P_LINEORDER",
                                                     "actual_table": "SSB_SF1.P_LINEORDER",
                                                     "dim_cols": [{"name": "LO_ORDERDATE",
                                                                   "desc": "",
                                                                   "type": 0,
                                                                   "data_type": "integer",
                                                                   "alias": "LO_ORDERDATE",
                                                                   "is_visible": True,
                                                                   "order": -1,
                                                                   "invisible": []}],
                                                     "hierarchys": []}],
                               "measures": [{"name": "COUNT_ALL",
                                             "desc": "",
                                             "data_type": "",
                                             "expression": "COUNT",
                                             "dim_column": "1",
                                             "alias": "COUNT_ALL",
                                             "is_visible": True,
                                             "order": -1,
                                             "invisible": []}]}],
                   "calculate_measures": [],
                   "named_sets": [],
                   "dim_table_model_relations": [{"model_name": "botestdataset",
                                                  "table_relations": [{"table_name": "P_LINEORDER",
                                                                       "relation_type": 0,
                                                                       "relation_fact_key": "",
                                                                       "relation_bridge_table_name": ""}]}],
                   "canvas": "{'models':[]}"}

headers = {'Accept': 'application/vnd.apache.kylin-v2+json',
                         'Accept-Language': 'en',
                         'Content-Type': 'application/json;charset=utf-8'
                         }
auth = ('ADMIN', 'kylin@2020')
url = "http://10.1.2.95:9002/kylin/dataset"
resp = requests.request("POST", url, headers=headers, auth=auth, json=payload)
if __name__ == '__main__':

    try:
        data = resp.json()
        resp.raise_for_status()
    except HTTPError as http_error:
        error_msg = f"{data.get('message')}\n" \
                    f"{str(http_error)}"
        logging.error(error_msg)