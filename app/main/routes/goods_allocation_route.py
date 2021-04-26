# -*- coding: utf-8 -*-
# Description: 分货
# Created: shaoluyu 2020/06/16
import json
from flask import request, current_app
from flask_restful import Resource
from app.main.steel_factory.service.dispatch_service import dispatch, save_load_task
from app.main.steel_factory.service.feedback_service import service
from app.main.steel_factory.service.stock_service import deal_stock
from app.util.my_exception import MyException
from app.util.result import Result


class GoodsAllocationRoute(Resource):

    @staticmethod
    def post():
        """输入订单，返回开单结果
        """
        try:
            json_data = json.loads(request.get_data().decode("utf-8"))
            # 业务id列表
            id_list = [json_data.get("company_id", ""), json_data.get("create_id", ""),
                       json_data.get("cargo_split_id", "")]
            # 可发库存数据列表
            data = json_data["data"]
            # 库存处理
            stock_list, sift_stock_list = deal_stock(data)
            # 配载
            load_task_list = dispatch(stock_list, sift_stock_list)
        except MyException as me:
            # 调用反馈接口,模型错误
            service(Result.error(me.message), [])
            current_app.logger.exception(me)
            current_app.logger.error(me.message)
            return Result.error_response()
        except Exception as e:
            # 调用反馈接口,模型错误
            service(Result.error(), [])
            current_app.logger.exception(e)
            return Result.error_response()
        else:
            current_app.logger.info('分货成功，准备进行反馈')
            # 调用反馈接口，模型成功
            service(Result.success(data=load_task_list), id_list)
            # 写库
            save_load_task(load_task_list, id_list)
            return Result.success_response()
