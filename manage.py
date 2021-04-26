#!/usr/bin/python
# -*- coding: utf-8 -*-
# Description: Flask应用入口
# Created: shaoluyu 2019/10/29
# Modified: shaoluyu 2019/10/29; shaoluyu 2019/06/20

import logging
import os

import config
from logging.handlers import TimedRotatingFileHandler
from app import create_app

# 获取当前生效的环境配置类
active_config_name = config.get_active_config_name()
# 创建flask应用
app = create_app(active_config_name)

if __name__ != '__main__':
    # 如果不是直接运行，而是gunicorn运行，则将日志输出到 gunicorn 中
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    # 日志记录当前环境配置名称
    app.logger.info('active config name = {} '.format(active_config_name))

if __name__ == '__main__':
    # 日志记录当前环境配置名称
    app.logger.info('flask app name = {} '.format(app.name))
    app.logger.info('active config name = {} '.format(active_config_name))
    # from waitress import serve
    # serve(app, listen='*:9238')
    # 启动flask应用app
    server_port = app.config.get('SERVER_PORT')
    app.run(host='127.0.0.1', port=server_port, debug=True)
