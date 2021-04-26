from pandas import DataFrame
import numpy as np

if __name__ == '__main__':
    df1 = DataFrame(np.arange(16).reshape((4, 4)), index=['a', 'b', 'c', 'd'],
                    columns=['one', 'two', 'three', 'four'])  # 创建一个dataframe
    print(df1)
    df1.loc['e'] = 0  # 优雅地增加一行全0
    df1.loc['f'] = 4
    df1['five'] = 0.0
    print(df1)
    print("-----------------")
    df1['five'].replace(0.0, np.nan)
    print(df1)
    print(df1['five'].empty)
    # df1.dropna(subset=['five'])
    # print(df1)
    # print(df1['five'].isna())


