# -*- coding: utf-8 -*-
# Description: Flask应用
# Created: shaoluyu 2019/10/29
# Modified: shaoluyu 2019/10/29; shaoluyu 2019/06/20

import atexit
from flask import Flask
from flask_apscheduler import APScheduler
import platform
from app.util.aspect import *
# from app.util.eureka_client_util import EurekaClientUtil
from config import config, Config
from app.util.core import JSONEncoder
from app.util.actuator import Actuator

# SQLAlchemy数据库访问实例
# db = SQLAlchemy()

# APScheduler任务调度器
scheduler = APScheduler()


def _access_control(response):
    """解决跨域请求

    :param response:
    :return:
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = (
        'GET,HEAD,PUT,PATCH,POST,DELETE')
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    response.headers['Access-Control-Max-Age'] = 86400
    return response


def create_app(config_name):
    """根据环境配置名称，创建Flask应用。

    :param config_name: 环境配置名称
    :return: app Flask应用
    """
    # __name__ 当前模块名称
    app_name = getattr(Config, 'APP_NAME', __name__)
    app = Flask(app_name)

    # 初始化app配置
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)

    # 返回json格式转换
    app.json_encoder = JSONEncoder

    # 解决跨域
    app.after_request(_access_control)

    # 数据库连接初始化
    # if app.config.get('SQLALCHEMY_DATABASE_URI'):
    #     db.init_app(app)

    # 启动定时任务
    if app.config.get('SCHEDULER_OPEN'):
        scheduler_init(app)

    # 初始化蓝本(路由)
    from .main import blueprint
    app.register_blueprint(blueprint, url_prefix='')

    # 初始化应用监控
    Actuator.init_app(app)

    # 服务注册与发现
    # EurekaClientUtil.set_eureka()

    return app


def scheduler_init(app):
    """初始化任务调度器

    根据应用配置，为APScheduler初始化定时任务。
    使用文件锁，保证一个服务器只启动一次定时任务。
    :param app: Flask应用
    :return: 无
    """

    lock_file_name = app.config.get("SCHEDULER_LOCK_FILE_NAME")
    if lock_file_name is None:
        app.logger.error('初始化任务调度器，配置中缺少任务调度器lock文件名称！！')
        raise Exception('配置中缺少任务调度器lock文件名称！')

    if platform.system() != 'Windows':
        fcntl = __import__("fcntl")
        # 打开（或创建）一个lock文件
        f = open(lock_file_name, 'wb')
        try:
            # 在lock文件上加上非阻塞互斥锁
            # 成功后初始化scheduler并启动
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            scheduler.init_app(app)
            scheduler.start()
            app.logger.info('Scheduler Started ---------------')
        except:
            pass

        def unlock():
            fcntl.flock(f, fcntl.LOCK_UN)
            f.close()
            app.logger.info('Scheduler Exited ---------------')

        # 注册退出事件
        # 当flask项目退出时，解锁并关闭lock文件的锁。
        atexit.register(unlock)
    else:
        msvcrt = __import__('msvcrt')
        f = open(lock_file_name, 'wb')
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            scheduler.init_app(app)
            scheduler.start()
            app.logger.info('Scheduler Started ----------------')
        except:
            pass

        def _unlock_file():
            try:
                f.seek(0)
                msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
                app.logger.info('Scheduler Exited ---------------')
            except:
                pass

        # 注册退出事件
        # 当flask项目退出时，解锁并关闭lock文件的锁。
        atexit.register(_unlock_file)
