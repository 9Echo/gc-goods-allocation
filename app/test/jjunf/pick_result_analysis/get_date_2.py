# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/11/15
import datetime

from app.test.jjunf.pick_result_analysis.get_date import get_date, get_date_ymd, get_date_before_830
from app.test.jjunf.pick_result_analysis.pick_analysis_config import PickAnalysisConfig


def get_the_day_now():
    """
    返回当天的日期：年-月-日  00:00:00
    :return:
    """
    # 获取当天的日期：年-月-日  00:00:00
    return get_date() - datetime.timedelta(days=PickAnalysisConfig.DAY_NUMBER - 1)


def get_one_day_before_now(day=0):
    """
    返回前一天的日期：年-月-日  00:00:00
    :return:
    """
    # 获取前一天的日期：年-月-日  00:00:00
    return get_date() - datetime.timedelta(days=PickAnalysisConfig.DAY_NUMBER + day)


def get_one_day_before_now_ymd():
    a = get_date_ymd() - datetime.timedelta(days=PickAnalysisConfig.DAY_NUMBER)
    return a


def get_one_day_before_830():
    return get_date_before_830() - datetime.timedelta(days=PickAnalysisConfig.DAY_NUMBER)


def get_time_interval():
    """
    司机id相同时，判断是否是一个车次的时间间隔
    :return:
    """
    return datetime.timedelta(hours=PickAnalysisConfig.TIME_INTERVAL_HOUR)
