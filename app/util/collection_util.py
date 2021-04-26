# -*- coding: utf-8 -*-
# Description: 集合工具
# Created: shaoluyu 2020/02/21
import copy

from app.util.code import ResponseCode
from app.util.my_exception import MyException
import numpy as np


def dot(left_list, right_list):
    """
    list按位相乘的和
    :param left_list:
    :param right_list:
    :return:
    """
    left_list = copy.copy(left_list)
    right_list = copy.copy(right_list)
    if left_list is None or right_list is None:
        raise MyException('存在None', ResponseCode.Error)
    if isinstance(left_list, np.ndarray):
        left_list = left_list.tolist()
    if isinstance(right_list, np.ndarray):
        left_list = left_list.tolist()
    if not isinstance(left_list, list) or not isinstance(right_list, list):
        raise MyException('存在非list类型', ResponseCode.Error)
    if len(left_list) != len(right_list):
        raise MyException('两者长度不相等', ResponseCode.Error)
    try:
        return sum((np.multiply(np.array(left_list), np.array(right_list))).tolist())
    except TypeError:
        raise MyException('元素类型错误', ResponseCode.Error)
