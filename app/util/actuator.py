# -*- coding: utf-8 -*-
# Description: 应用管理服务
# Created: shaoluyu 2019/07/05
# Modified: shaoluyu 2019/07/06;

import logging
import os
from datetime import datetime

from flask import jsonify

from config import Config


class Actuator(object):
    """应用管理类

    提供info, health, metrics等管理方法
    """

    # 启动时间，字符串类型，格式'%Y-%m-%d %H:%M:%S'
    _start_time = None

    @classmethod
    def set_start_time(cls, start_time):
        """设置启动时间，字符串类型，格式'%Y-%m-%d %H:%M:%S'
        """
        if not cls._start_time:
            cls._start_time = start_time
        # 日志记录应用启动时间
        gunicorn_logger = logging.getLogger('gunicorn.error')
        if gunicorn_logger:
            gunicorn_logger.info('应用启动时间：{}'.format(cls._start_time))
        # print('应用启动时间：{} \n'.format(cls._start_time))

    @staticmethod
    def info():
        """应用说明
        """
        # 获取环境配置名称
        config_name = os.getenv('FLASK_CONFIG') or 'default'
        return jsonify({"name": Config.APP_NAME, "config_name": config_name})

    @classmethod
    def health(cls):
        """应用健康状态
        """
        return jsonify({"status": "UP"})

    @classmethod
    def metrics(cls):
        """应用度量
        """
        return jsonify({"start_time": cls._start_time})

    @classmethod
    def init_app(cls, flask_app):
        # 获取和设置启动时间
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cls.set_start_time(start_time)
        # 设置应用监控端点
        flask_app.add_url_rule(
            '/info', endpoint='info', view_func=Actuator.info)
        flask_app.add_url_rule(
            '/health', endpoint='health', view_func=cls.health)
        flask_app.add_url_rule(
            '/metrics', endpoint='metrics', view_func=cls.metrics)
