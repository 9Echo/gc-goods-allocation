# -*- coding: utf-8 -*-
# @Time    : 2019/11/15 16:17
# @Author  : Zihao.Liu
from flask import request
from flask_restful import Resource
from app.main.pipe_factory.service.compose_service import compose
from app.util.result import Result


class ComposeRoute(Resource):
    """
    拼单推荐接口
    """
    @staticmethod
    def post():
        """进行拼货推荐"""
        if request.get_data():
            # 获取输入参数（发货通知单列表）
            delivery_list_data = request.get_json(force=True).get('items')  # 入参是json
            if delivery_list_data:
                result_delivery_list = compose(delivery_list_data)
                return Result.success_response(result_delivery_list)
            else:
                return Result.error_response('数据为空！')





