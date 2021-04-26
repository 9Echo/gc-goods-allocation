# -*- coding: utf-8 -*-
# Description: 摘单推荐服务通用方法类
# Created: luchengkai 2020/11/16
from collections import defaultdict
from typing import Dict, List


def split_group(data_list, attr_list) -> Dict[str, List]:
    """
    将data_list按照attr_list属性list分组
    :param attr_list:
    :param data_list:
    :return:
    """
    # 结果字典：{‘attr_list’：[分组列表]}
    result_dict = defaultdict(list)
    for data in data_list:
        key = getattr(data, attr_list[0], '未知参数')
        for i in range(1, len(attr_list)):
            key = key + ', ' + getattr(data, attr_list[i], '未知参数')
        result_dict[key].append(data)
    return result_dict

