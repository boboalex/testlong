import json

if __name__ == '__main__':
    with open('table_columns.json') as fp:
        # 通过json.load解析为字典
        table_columns_dict = json.load(fp)
        # print(table_columns_dict['database_name'])
    columns_str = ""
    for key, value in table_columns_dict['columns'].items():
        columns_str = columns_str + f"`{key}`" + "\t" + f"{value}" + "," + "\n"
    # 清除最后一个逗号和换行符
    columns_str = columns_str.strip(",\n")
    # print(columns_str)
    create_str = f"create table `{table_columns_dict['database_name']}.{table_columns_dict['table_name']}`(\n " \
                 f"{columns_str} \n" \
                 f") row format delimited fields terminated by ',';"
    print(create_str)