# -*- coding: utf-8 -*-
# Description:计算百分比，并且按照digit指定的位数保留小数位数
# Created: jjunf 2020/11/17
import math


def percent_util(number1, number2, digit=0):
    """
    计算百分比，并且按照digit指定的位数保留小数位数
    :param number1: 分子
    :param number2: 分母
    :param digit: 百分比中小数点后保留的位数，默认保留0位小数
    :return:
    """
    number1 = float(number1)
    number2 = float(number2)
    digit = int(digit)
    result = (number1 / number2) * math.pow(10, digit + 2)
    if result >= float(int(result)) + 0.5:
        result = math.ceil(result)
    else:
        result = math.floor(result)
    if digit != 0:
        result = float(result) / math.pow(10, digit)
    if digit < 0:
        result = int(result)
    elif digit > 0 and digit > len(str(result).split('.')[1]):
        for i in range(digit - len(str(result).split('.')[1])):
            result = str(result) + '0'
    return str(result) + '%'
