# -*- coding: utf-8 -*-
# Description: 单车配载
# Created: shaoluyu 2020/06/16
import json

import flask_restful
from flask import request
from flask_restful import Resource, reqparse
from app.main.steel_factory.service.single_dispatch_service import dispatch
from app.main.steel_factory.service.truck_service import generate_truck
from app.util.my_abort import MyAbort
from app.util.my_flask_verify import MyFlaskVerify
from app.util.result import Result

flask_restful.abort = MyAbort.my_abort


class SingleGoodsAllocationRoute(Resource):

    @staticmethod
    def post():
        """
        输入车辆信息，返回开单结果
        """
        parser = reqparse.RequestParser(trim=True)
        parser.add_argument('schedule_no', type=MyFlaskVerify.str_type, help='报道号验证不通过！', required=True, nullable=False,
                            location=['json'])
        parser.add_argument('load_weight', type=MyFlaskVerify.number_type, help='载重验证不通过！', required=True,
                            nullable=False, location=['json'])
        # 验证大于0
        parser.add_argument('load_weight', type=MyFlaskVerify.number_value, help='载重验证不通过！', required=True,
                            nullable=False, location=['json'])
        parser.parse_args()
        json_data = json.loads(request.get_data().decode("utf-8"))
        truck = generate_truck(json_data)
        result = dispatch(truck)
        return Result.success_response(result)
