# -*- coding: utf-8 -*-
# Description: Gunicorn服务器配置文件
# Created: shaoluyu 2019/10/29
# Modified: shaoluyu 2019/10/29

from gevent import monkey

# 单进程的异步编程模型称为协程。gevent是把python同步代码变成异步协程的第三方库。
# gevent的monkey patch在执行时将标准库中的thread/socket等动态替换为非阻塞的模块。
monkey.patch_all()

import multiprocessing

from app.util.file_handlers import MultiProcessSafeTimedRotatingFileHandler

# 应用参数
from config import Config

APP_NAME = Config.APP_NAME
SERVER_PORT = Config.SERVER_PORT

# 配置参数
bind = '0.0.0.0:{}'.format(SERVER_PORT)
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = 'gevent'
worker_connections = 1000
backlog = 2048
timeout = 120
debug = True
proc_name = 'gunicorn_app'
pidfile = '/app/{}/logs/gunicorn.pid'.format(APP_NAME)
capture_output = True
# logfile = '/app/{}/logs/gunicorn.log'.format(APP_NAME)

# 访问日志格式，错误日志无法设置
"""
其每个选项的含义如下：
h          remote address
l          '-'
u          currently '-', may be main name in future releases
t          date of the request
r          status line (e.g. ``GET / HTTP/1.1``)
s          status
b          response length or '-'
f          referer
a          main agent
T          request time in seconds
D          request time in microseconds
L          request time in decimal seconds
p          process ID
"""

# access_log_format = '%(t)s %(p)s %(h)s "%(r)s" %(s)s %(L)s %(b)s %(f)s" "%(a)s"'
# # 访问日志文件
# accesslog = '/app/{}/logs/gunicorn_access.log'.format(APP_NAME)
#
# # 错误日志级别，访问日志级别无法设置
# loglevel = 'debug'
# # 错误日志文件
# errorlog = '/app/{}/logs/gunicorn_server.log'.format(APP_NAME)

logconfig_dict = {
    'version': 1,
    'disable_existing_loggers': False,
    'root': {
        "level": "DEBUG",  # 打日志的等级可以换的，下面的同理
        # "handlers": ["error_file"],  # 对应下面的键
        # "propagate": 1
    },
    'loggers': {
        "gunicorn.error": {
            "level": "DEBUG",  # 打日志的等级可以换的，下面的同理
            "handlers": ["error_file"],  # 对应下面的键
            "propagate": 1,
            "qualname": "gunicorn.error"
        },
        "gunicorn.access": {
            "level": "DEBUG",
            "handlers": ["access_file"],
            "propagate": 0,
            "qualname": "gunicorn.access"
        }
    },
    'handlers': {
        "error_file": {
            "class": "app.util.file_handlers.MultiProcessSafeTimedRotatingFileHandler",
            "when": "midnight",
            "interval": 1,
            "backupCount": 14,  # 备份多少份
            "formatter": "generic",  # 对应下面的键
            # 'mode': 'w+',
            "filename": '/app/{}/logs/gunicorn_server.log'.format(APP_NAME),  # 日志文件路径
            'encoding': 'utf-8'  # 日志文件编码
        },
        "access_file": {
            "class": "app.util.file_handlers.MultiProcessSafeTimedRotatingFileHandler",
            "when": "midnight",
            "interval": 1,
            "backupCount": 14,
            "formatter": "access",
            "filename": '/app/{}/logs/gunicorn_access.log'.format(APP_NAME),
            'encoding': 'utf-8'
        }
    },
    'formatters': {
        "generic": {
            "format": "[%(asctime)s.%(msecs)03d] [%(levelname)s] [%(process)d] [%(filename)s:%(lineno)s] - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",  # 时间显示格式
            "class": "logging.Formatter"
        },
        "access": {
            "format": "[%(asctime)s.%(msecs)03d] [%(levelname)s] [%(process)d] [%(filename)s:%(lineno)s] - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",  # 时间显示格式
            "class": "logging.Formatter"
        }
    }
}
