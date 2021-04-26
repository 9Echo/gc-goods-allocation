from math import isnan

import pandas as pd


def test_df():
    # data = [['Alex', 10], ['Bob', 12], ['Clarke', 13]]
    # df = pd.DataFrame(data, columns=['Name', 'Age'], dtype=float)
    # print('list list to df:\n', df)
    #
    # data = (('Alex', 10), ('Bob', 12), ('Clarke', 13))
    # df = pd.DataFrame(list(data), columns=['Name', 'Age'], dtype=float)
    # print('tuple list to df:\n', df)
    #
    # data = {'Name': ['Tom', 'Jack', 'Steve', 'Ricky'], 'Age': [28, 34, 29, 42]}
    # df = pd.DataFrame(data)
    # print('list dict to df:\n', df)
    # df = pd.DataFrame(data, index=['rank1', 'rank2', 'rank3', 'rank4'])
    # print('list dict to df2:\n', df)

    data = [{'a': 1, 'b': 2}, {'a': 5, 'b': None}, {'a': 1, 'b': 10}, {'a': 5, 'b': 10}, {'a': 5, 'b': 10}]
    df = pd.DataFrame(data)
    for index, row in df.iterrows():
        print(index, row['b'])
        print(isnan(row['b']))
    print('dict list to df:\n', df)
    group = df.groupby(by=['a'])
    print(group)
    # for k, v in group.items():
    #     print(k)
    #     print(v)
    # print(type(group))
    # print(data)
    print(group.groups)
    print(type(group.groups))
    for k, v in group.groups.items():
        print(k)
        print(v)
        print(type(v))
        for i in list(v):
            print(i)

    # g.DOC_TYPE = '提货单'
    # delivery_no = 0
    # dict_list = [i.as_dict() for i in sheets]
    # df = pd.DataFrame(dict_list)
    # group = df.groupby(df['load_task_id'])
    # # 遍历每个车次
    # for k, v in group.groups.items():
    #     one_load_task_delivery_list = []
    #     # 根据下标定位，取出同车次的提货单
    #     for i in list(v):
    #         one_load_task_delivery_list.append(sheets[i])
    #     while one_load_task_delivery_list:
    #         temp_delivery_list = product_type_rule.filter(one_load_task_delivery_list)
    #         delivery_no += 1
    #         for j in temp_delivery_list:
    #             j.delivery_no = g.DOC_TYPE + str(delivery_no)
    #             one_load_task_delivery_list.remove(j)
    # data = [{'a': 1, 'b': 2}, {'a': 5, 'b': 10, 'c': 20}, {'a': 10, 'b': 11}, {'a': 20, 'b': 21}]
    # df = pd.DataFrame(data, index=['first', 'second', 'third', 'fourth'])
    # print('dict list to df2:\n', df)
    # print('dict list to df2 -> slice:\n', df[1:4])
    # print('dict list to df2 -> value:\n', df.values)
    # print('dict list to df2 -> row:\n', df.iloc[1])


if __name__ == "__main__":
    test_df()
