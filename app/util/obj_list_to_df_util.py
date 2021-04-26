# -*- coding: utf-8 -*-
# Description: 将列表对象转换成DataFrame对象
# Created: jjunf 2021/01/14
from collections import defaultdict
from typing import List, Dict

import pandas as pd


def obj_list_to_df_util(obj_list: List[object], attr_dict: Dict):
    """
    将列表中的所有对象在属性字典中的属性转换成DataFrame对象返回（可方便导出为excel）
    :param obj_list: 对象列表
    :param attr_dict: 属性字典：键为属性；值为属性名称，即sheet的title
    :return:
    """
    df = pd.DataFrame()
    dic = defaultdict(list)
    for obj in obj_list:
        # 按属性字典只导出需要的字段
        for attr in attr_dict.keys():
            # 验证传入属性字典的属性是否是列表对象的属性
            if attr in obj.__dict__.keys():
                key = attr_dict.get(attr, attr)
                dic[key].append(getattr(obj, attr))
    # key_list: List = list(dic.keys())
    # key_list.reverse()
    # # 转置一下，因为每次插入0位置，先插入的会跑到后面去
    # for key in key_list:
    #     df.insert(0, key, dic[key])
    for key in dic.keys():
        df[key] = dic[key]
    return df
