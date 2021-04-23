import logging
import requests
from configparser import ConfigParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
cfg = ConfigParser()
cfg.read('config.ini')


class BasicHttpClient:
    _ki_project = cfg.get('server', 'ki_project')
    _ki_create_dataset = cfg.get('project', 'create_dataset_name')

    _headers = {}

    _auth = ('ADMIN', 'kylin@2020')

    _inner_session = requests.Session()

    def __init__(self):
        self._ki_host = cfg.get('server', 'ki_host')
        self._ki_port = cfg.get('server', 'ki_port')

        if not self._ki_host or not self._ki_port:
            raise ValueError('init http client failed, the host and the port is None!')
        # self._base_url = self._base_url.format(host=self.host, port=self.port)

    # def __init__(self):
    #     # host = cfg.get('server', 'host')
    #     # port = cfg.get('server', 'port')
    #     if not self.host or self.port:
    #         # raise ValueError('i/nitializing http client failed')
    #         print(host)
    #         print("host is none")
    #     self._host = host
    #     self._port = port

    def token(self, token):
        self._headers['Authorization'] = 'Basic {token}'.format(token=token)

    def auth(self, username, password):
        self._auth = (username, password)

    def header(self, name, value):
        self._headers[name] = value

    def headers(self, headers):
        self._headers = headers

    def _request(self, method, url, params=None, data=None, json=None,  # pylint: disable=too-many-arguments
                 files=None, headers=None, stream=False, to_json=True, inner_session=False, timeout=60):
        if inner_session:
            return self._request_with_session(self._inner_session, method, url,
                                              params=params,
                                              data=data,
                                              json=json,
                                              files=files,
                                              headers=headers,
                                              stream=stream,
                                              to_json=to_json,
                                              timeout=timeout)
        with requests.Session() as session:
            session.auth = self._auth
            return self._request_with_session(session, method, url,
                                              params=params,
                                              data=data,
                                              json=json,
                                              files=files,
                                              headers=headers,
                                              stream=stream,
                                              to_json=to_json,
                                              timeout=timeout)

    def _request_with_session(self, session, method, url, params=None, data=None,  # pylint: disable=too-many-arguments
                              json=None, files=None, headers=None, stream=False, to_json=True, timeout=60):
        if headers is None:
            headers = self._headers
        resp = session.request(method, url,
                               params=params,
                               data=data,
                               json=json,
                               files=files,
                               headers=headers,
                               stream=stream,
                               timeout=timeout
                               )

        try:
            if stream:
                return resp.raw
            if not resp.content:
                return None

            if to_json:
                # 注意实际上并不包含状态码等信息，只是包含返回的结果
                data = resp.json()
                resp.raise_for_status()
                return data
            return resp.text
        except requests.HTTPError as http_error:
            # KI的HTTPError返回的字典中一般只有一种key：error或message，即{'error'/'message': [报错信息]}
            err_msg = f"{data.get('error')}\n" \
                      f"{data.get('message')}\n" \
                      f"{str(http_error)}"
            logging.error(err_msg)
            raise requests.HTTPError(err_msg, request=http_error.request, response=http_error.response, )
        except Exception as error:
            logging.error(str(error))
            raise error
