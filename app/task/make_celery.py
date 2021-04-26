# -*- coding: utf-8 -*-
# Description: Celery应用管理
# Created: shaoluyu 2019/10/14
# Modified: shaoluyu 2019/10/16;

import logging.config

from app import create_app
from config import Config, get_active_config, get_active_config_name


def make_celery(flask_app=None):
    """创建Celery应用。
    Celery是分布式任务系统，支持异步任务执行和定时任务执行。

    :param flask_app: 要关联到Celery应用的Flask应用
    :return: 创建的Celery应用
    """
    active_config = get_active_config()
    if not hasattr(active_config, 'CELERY_BROKER_URL'):
        return None
    #
    active_config_name = get_active_config_name()
    flask_app = flask_app or create_app(active_config_name)
    #
    from celery import Celery
    celery_app_name = 'celery-{}'.format(active_config.APP_NAME)
    celery_app = Celery(celery_app_name,
                        broker=active_config.CELERY_BROKER_URL,
                        backend=getattr(active_config, 'CELERY_RESULT_BACKEND', None)
                        )
    #
    celery_app.conf.update(flask_app.config)
    #
    TaskBase = celery_app.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with flask_app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)

    celery_app.Task = ContextTask
    #
    return celery_app


# Celery执行器日志文件名称
app_name = Config.APP_NAME
active_config_name = get_active_config_name()
celery_log_file_name = '/app/{}/logs/Celery-log.log'.format(app_name)
if active_config_name == 'default':
    celery_log_file_name = 'celery-log-{}.log'.format(app_name)
# Celery执行器日志等级 DEBUG < INFO < WARNING < ERROR < CRITICAL
celery_log_level = 'DEBUG'

LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'loggers': {
        'celery_worker': {
            'handlers': ['celery_file'],
            'level': celery_log_level,
            'propagate': True,
        }
    },
    'handlers': {
        'celery_file': {
            "class": "app.util.file_handlers.MultiProcessSafeTimedRotatingFileHandler",
            "when": "midnight",
            "interval": 1,
            "backupCount": 14,  # 备份多少份
            "formatter": "generic",  # 对应下面的键
            # 'mode': 'w+',
            'filename': celery_log_file_name,  # 日志文件路径
            'encoding': 'utf-8'  # 日志文件编码
        },
    },
    'formatters': {
        'generic': {
            "format": "[%(asctime)s.%(msecs)03d] [%(levelname)s] [%(process)d] [%(filename)s:%(lineno)s] - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",  # 时间显示格式
        }
    }
}

# 日志配置中设置Celery执行器日志
logging.config.dictConfig(LOG_CONFIG)
