# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/11/14

from datetime import datetime


def get_date():
    """
    返回当前时间：年-月-日  00:00:00
    :return:
    """
    time_str = datetime.now().strftime("%Y-%m-%d ") + '00:00:00'
    return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")


def get_date_ymd():
    """
    返回当前时间：年月日
    :return:
    """
    time_str = datetime.now().strftime("%Y%m%d")
    return datetime.strptime(time_str, "%Y%m%d")


def get_date_before_830():
    """
    返回：年-月-日  08:30:00
    :return:
    """
    time_str = datetime.now().strftime("%Y-%m-%d ") + '08:30:00'
    return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")


if __name__=='__main__':
    a=get_date_ymd()
    print(a)
    1