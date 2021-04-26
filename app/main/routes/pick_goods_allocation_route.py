# -*- coding: utf-8 -*-
# Description: 摘单分货
# Created: jjunf 2020/10/15
import json
import flask_restful
from flask import request
from flask_restful import Resource
from app.main.steel_factory.service.pick_goods_dispatch_service import dispatch
from app.util.my_abort import MyAbort
from app.util.result import Result

flask_restful.abort = MyAbort.my_abort


class PickGoodsAllocation(Resource):

    @staticmethod
    def post():
        """
        输入json库存信息，返回分好的摘单记录
        """
        json_data = json.loads(request.get_data().decode("utf-8"))
        if not json_data["data"]:
            return Result.error('列表为空！')
        result = dispatch(json_data["data"])
        return Result.success_response(result)
