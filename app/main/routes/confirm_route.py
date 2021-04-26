# -*- coding: utf-8 -*-
# Description: 发货通知单确认
# Created: shaoluyu 2019/11/13
# Modified: shaoluyu 2019/11/13
from flask import request
from flask_restful import Resource
from app.main.pipe_factory.service.confirm_delivery_service import confirm
from app.main.pipe_factory.service.confirm_delivery_service import generate_delivery
from app.util.result import Result


class ConfirmRoute(Resource):
    """
       确认发货通知单
       """
    @staticmethod
    def post():
        """
        获取人工确认后的发货通知单
        对比分析保存差异信息
        :return:
        """
        if request.get_data():
            # 获取输入参数
            delivery_data = request.get_json(force=True).get('data')  # 入参是json
            # 创建发货通知单实例，初始化属性
            if delivery_data:
                delivery_item_list = generate_delivery(delivery_data)
                # 对比
                confirm(delivery_data['company_id'], delivery_data['batch_no'], delivery_item_list)
                return Result.success_response({})
            else:
                return Result.error_response('数据为空！')
