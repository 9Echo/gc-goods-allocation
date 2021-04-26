# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/10/21 9:48
import math


def round_util(number):
    number = float(number)
    if number >= float(int(number)) + 0.5:
        return math.ceil(number)
    else:
        return math.floor(number)


def round_util_by_digit(number, digit=0):
    """
    按照digit指定的位数保留小数位数
    :param number:
    :param digit: 小数点后保留的位数，默认保留0位小数
    :return:四舍五入后的字符串
    """
    number = float(number)
    digit = int(digit)
    result = number * pow(10, digit)
    result = round_util(result)
    if digit != 0:
        result = float(result) / math.pow(10, digit)
    if digit < 0:
        result = int(result)
    elif digit > 0 and digit > len(str(result).split('.')[1]):
        for i in range(digit - len(str(result).split('.')[1])):
            result = str(result) + '0'
    return str(result)
