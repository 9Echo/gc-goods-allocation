# -*- coding: utf-8 -*-
# Description: 订单请求
# Created: shaoluyu 2019/11/13
# Modified: shaoluyu 2019/11/13
import json
from threading import Thread
from flask import request
from flask_restful import Resource
from app.main.pipe_factory.service import order_service
from app.main.pipe_factory.service.dispatch_service import dispatch_spec, dispatch_weight, dispatch_optimize
from app.main.pipe_factory.service.sheet_service import save_sheets
from app.util.result import Result


class OrderRoute(Resource):

    @staticmethod
    def post():
        """输入订单，返回开单结果
        """
        if request.get_data():
            order_data = json.loads(request.get_data().decode("utf-8"))
            # 数据初始化
            order = order_service.generate_order(order_data['data'])
            # 携带参数
            request_id = order_data['data'].get('request_id', None)
            # 透传信息
            ext_info_map = order_data['data'].get('ext_info_map', None)
            # 规格优先
            sheets_spec = dispatch_spec(order)
            # 重量优先
            sheets_weight = dispatch_weight(order)
            # 综合
            sheets_recommend = dispatch_optimize(order)
            result_dict = dict()
            result_dict['spec_first'] = sheets_spec
            result_dict['weight_first'] = sheets_weight
            result_dict['recommend_first'] = sheets_recommend
            result_dict['request_id'] = request_id
            result_dict['ext_info_map'] = ext_info_map
            try:
                return Result.success_response(result_dict)
            finally:
                Thread(target=save_sheets, args=(sheets_spec + sheets_weight + sheets_recommend,)).start()
