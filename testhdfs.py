from hdfs.client import Client
from hdfs import InsecureClient
if __name__ == '__main__':
    # try:
    #     client = Client("http://10.1.2.32:50070", root='/')
    #     # in_client = InsecureClient("http://10.1.3.183:50070", user='hdfs')
    #     # with open('/Users/bo.zhang/Documents/scripts/testlong/config.ini') as reader, in_client.write('/tmp/config.ini') as writer:
    #     #     for line in reader:
    #     #         writer.write(line.encode('utf-8'))
    #     client.upload('/tmp', 'data.csv')
    # except Exception as e:
    #     # print(client)
    #     pass
    # client = Client("http://10.1.3.183:50070", root='/')
    # client.upload('/tmp', 'data.csv')
    from pyhive import hive
    import pandas as pd

    # open connection
    conn = hive.Connection(host='10.1.3.183', port=10000, username='root', database='bo_test')

    # query the table to a new dataframe
    dataframe = pd.read_sql("SELECT * FROM xxx limit 2", conn)
    print(type(dataframe))
    print(dataframe)
