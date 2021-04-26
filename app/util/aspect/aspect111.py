# -*- coding: utf-8 -*-
# Description: 切面
# Created: shaoluyu 2019/03/05
import json
from flask import request
from datetime import datetime
from app.main import blueprint
from flask import g

from app.util.log_buffer_util import LogBufferUtil


@blueprint.before_request
def before_request():
    g.log_list = []
    g.start_time = datetime.now()
    LogBufferUtil.collect_log_list([
        '===========================start=============================',
        'method is ' + request.method,
        'url is ' + request.url,
        'body is ' + json.dumps(request.json, ensure_ascii=False),
        'ip is ' + request.remote_addr
    ])


@blueprint.after_request
def after_request(response):
    LogBufferUtil.collect_log_list([
        "body is " + json.dumps(response.json, ensure_ascii=False),
        "execution time:" + str(int((datetime.now() - g.start_time).total_seconds() * 1000)),
        '===========================end==============================='
    ])
    LogBufferUtil.output_log()
    return response
