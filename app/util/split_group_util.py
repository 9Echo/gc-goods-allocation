# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/12/23
from collections import defaultdict
from typing import List


def split_group_util(temp_list: List, attr_list: List, sep=','):
    """
    将列表temp_list按照属性列表attr_list中的属性分组。返回分组后的字典：键为分组属性用分隔符sep（默认逗号）拼接而成，值为传入的对象分组后的列表
    :param sep: 分隔符
    :param attr_list: 分组属性列表
    :param temp_list:需要分组的对象列表
    :return:分组后的字典
    """
    result_dict = defaultdict(list)
    for temp in temp_list:
        key_list = []
        for attr in attr_list:
            key_list.append(getattr(temp, attr))
        result_dict[sep.join(key_list)].append(temp)
    return result_dict
