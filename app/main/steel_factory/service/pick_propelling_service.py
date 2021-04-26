# -*- coding: utf-8 -*-
# Description:
# Created: luchengkai 2020/11/16 9:12
from celery.utils.log import get_task_logger

from app.main.steel_factory.rule import pick_propelling_rule, pick_propelling_label_rule, pick_save_result
from app.main.steel_factory.rule import pick_propelling_recall_screen_rule
from model_config import ModelConfig
from flask import json

# 获取celery执行器的日志记录器
logger = get_task_logger('celery_worker')


def propelling():
    """
    摘单计划推送入口
    :return:
    """
    """
    1.摘单计划筛选
    2.司机集获取(标签提取)
    3.司机集筛选(召回筛选)
    4.摘单计划与司机集合并
    """
    """1.摘单计划筛选"""
    res1 = None
    res2 = None
    wait_driver_list = []  # 待推送消息的司机列表
    wait_propelling_list = pick_propelling_rule.pick_list_filter()  # 待推送摘单计划列表
    logger.info('摘单列表：' + json.dumps([i.as_dict() for i in wait_propelling_list], ensure_ascii=False))
    exist_driver_list = pick_propelling_rule.pick_driver_list()  # 摘单计划中已经存在的司机集
    logger.info('已推送摘单的司机列表：' + json.dumps([i.as_dict() for i in exist_driver_list], ensure_ascii=False))
    # 待推送摘单计划中已存在的司机列表
    if wait_propelling_list:
        # 品名变更
        for wait_propelling in wait_propelling_list:
            tmp_prod = wait_propelling.prod_name.split(",")
            wait_propelling.prod_name = ModelConfig.PICK_REMARK.get(tmp_prod[0], '未知品种')
        """2.司机集获取(标签提取)"""
        wait_driver_list.extend(pick_propelling_label_rule.pick_label_extract(wait_propelling_list))
        """3.司机集筛选(召回筛选)"""
        propelling_driver_list = pick_propelling_recall_screen_rule.pick_recall_screen(wait_driver_list,
                                                                                       wait_propelling_list)
        """4.后台交互"""
        res1, res2 = pick_propelling_rule.interaction_with_java(propelling_driver_list, exist_driver_list)
        # 结果保存到数据库
        pick_save_result.save_propelling_log(propelling_driver_list)
    return res1, res2


if __name__ == '__main__':
    propelling()
