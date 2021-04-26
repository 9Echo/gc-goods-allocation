# -*- coding: utf-8 -*-
# Description: 切面
# Created: shaoluyu 2019/03/05
import json
from flask import request
from datetime import datetime
from app.main import blueprint
from flask import current_app, g


@blueprint.before_request
def before_request():
    g.start_time = datetime.now()
    current_app.logger.info('===========================start=============================')
    current_app.logger.info('before_request')
    current_app.logger.info('method is ' + request.method)
    current_app.logger.info('url is ' + request.url)
    current_app.logger.info('body is ' + json.dumps(request.json, ensure_ascii=False))
    current_app.logger.info('ip is ' + request.remote_addr)


@blueprint.after_request
def after_request(response):
    current_app.logger.info("after_request")
    current_app.logger.info("body is " + json.dumps(response.json, ensure_ascii=False))
    current_app.logger.info("execution time:" + str(int((datetime.now() - g.start_time).total_seconds() * 1000)))
    current_app.logger.info('===========================end===============================')
    return response
