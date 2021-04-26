# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/8/21 11:34

from flask_restful import abort

from app.util.code import ResponseCode


class MyAbort(object):
    @staticmethod
    def my_abort(http_status_code, *args, **kwargs):
        if http_status_code == 400:
            # 重定义400返回参数
            abort(400, **{"code": ResponseCode.Error, "msg": str(kwargs.get('message', ''))})
        abort(http_status_code)
