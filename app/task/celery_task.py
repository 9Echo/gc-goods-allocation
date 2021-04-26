# -*- coding: utf-8 -*-
# Description: Celery任务示例
# Created: shaoluyu 2019/10/14
# Modified: shaoluyu 2019/10/16; shaoluyu 2019/10/25;
import json
import traceback

from celery.utils.log import get_task_logger

from app.main.steel_factory.rule.no_interest_count_rule import blacklist_timer
from app.main.steel_factory.service.pick_propelling_service import propelling
from app.main.steel_factory.service.pick_save_hour_stock_service import save_hour_stock
from app.task.celery_app import celery

# 获取celery执行器的日志记录器
logger = get_task_logger('celery_worker')


@celery.task(ignore_result=True)
def push_message():
    try:
        logger.info('==========push_message_start=============')
        res1, res2 = propelling()
        logger.info(json.dumps(res1, ensure_ascii=False))
        logger.info(json.dumps(res2, ensure_ascii=False))
    except Exception as e:
        logger.error(traceback.format_exc())
    finally:
        logger.info('==========push_message_end===============')


@celery.task(ignore_result=True)
def check_driver_behavior():
    try:
        logger.info('==========check_driver_behavior_start=============')
        blacklist_timer()
    except Exception as e:
        logger.error(str(e))
    finally:
        logger.info('==========check_driver_behavior_end===============')


@celery.task(ignore_result=True)
def save_hour_stock_():
    logger.info('==========start===stock==========')
    save_hour_stock()

# @celery.task()
# def print_log():
#     logger.info('==========start=============')
