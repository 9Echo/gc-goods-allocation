# -*- coding: utf-8 -*-
# Description: 用户模块
# Created: shaoluyu 2019/10/29
# Modified: shaoluyu 2019/10/29; shaoluyu 2019/06/20

from flask import Blueprint
from flask import jsonify
from flask_restful import Api
# from app.main.routes.order_route import OrderRoute
# from app.main.routes.goods_allocation_route import GoodsAllocationRoute
from app.main.routes.pick_get_driver_list_route import PickGetDriverList
from app.main.routes.pick_goods_allocation_route import PickGoodsAllocation
# from app.main.routes.single_goods_allocation_route import SingleGoodsAllocationRoute

blueprint = Blueprint('main', __name__)
api = Api(blueprint)

# Routes
# 订单请求
# api.add_resource(OrderRoute, '/order')
# 订单请求测试
# api.add_resource(OrderRouteTest, '/orderTest')
# 推荐发货通知单确认反馈
# api.add_resource(ConfirmRoute, '/confirm')
# 拼单推荐
# api.add_resource(ComposeRoute, '/compose')
# 配载图请求
# api.add_resource(LoadingRoute, '/loading')
# 钢厂智能分货请求
# api.add_resource(GoodsAllocationRoute, '/goodsAllocation')
# 单车配货请求
# api.add_resource(SingleGoodsAllocationRoute, '/singleGoodsAllocation')
# 摘单分货请求
api.add_resource(PickGoodsAllocation, '/pickGoodsAllocation')
# 获取司机集请求
api.add_resource(PickGetDriverList, '/getDriverList')


@blueprint.route('/demo', methods=['GET'])
def demo():
    return jsonify({"name": "gc goods allocation"})
