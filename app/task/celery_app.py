# -*- coding: utf-8 -*-
# Description: 创建Celery应用
# Created: shaoluyu 2019/10/25
# Modified: shaoluyu 2019/10/25;
from celery.utils.log import get_task_logger

from app.task.make_celery import make_celery
import config

active_config = config.get_active_config()
# 获取celery执行器的日志记录器
logger = get_task_logger('celery_worker')
# 创建celery应用
celery = make_celery(flask_app=None)
# celery.conf.ONCE = {
#     'backend': 'celery_once.backends.Redis',
#     'settings': {
#         'url': active_config.REDIS_URL,
#         'default_timeout': active_config.default_timeout
#     }
# }


