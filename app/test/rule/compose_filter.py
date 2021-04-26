# -*- coding: utf-8 -*-
# @Time    : 2019/12/12
# @Author  : shaoluyu
from app.main.pipe_factory.rule import compose_weight_rule
from app.main.pipe_factory.dao.compose_dao import compose_dao
from app.util.code import ResponseCode
from app.util.my_exception import MyException
from model_config import ModelConfig
import pandas as pd


def filter(delivery_list_data: list):
    """
    拼单推荐逻辑
    :param delivery_list_data:
    :return:
    """

    # 客户列表
    customer_id_list = []
    # 发货通知单号列表
    delivery_no_list = []
    # 公司id
    company_id = ''
    # 现有重量
    weight = 0
    # 现有体积
    volume = 0
    for i in delivery_list_data:
        if not company_id:
            company_id = i.get('company_id')
        if i.get('delivery_no'):
            delivery_no_list.append(i.get('delivery_no'))
        if i.get('customer_id'):
            customer_id_list.append(i.get('customer_id'))
        weight += float(i.get('weight'))
        volume += int(i.get('quantity')) / ModelConfig.ITEM_ID_DICT.get(i.get('item_id')[:3], 10000)
    if not delivery_no_list:
        raise MyException('提货单号为空！', ResponseCode.Error)
    if not customer_id_list:
        raise MyException('客户为空！', ResponseCode.Error)
    if not company_id:
        raise MyException('公司id为空！', ResponseCode.Error)
    delivery_item_dict_list = compose_dao.get_compose_delivery(company_id, customer_id_list, delivery_no_list)
    if delivery_item_dict_list:
        delivery_dict_list = []
        df = pd.DataFrame(delivery_item_dict_list)
        group = df.groupby(by=['delivery_no'])
        for k, v in group:
            total_weight = 0
            total_volume = 0
            for i in v.to_dict(orient='records'):
                total_weight += i.get('weight', 0)
                total_volume += i.get('quantity', 0) / ModelConfig.ITEM_ID_DICT.get(i.get('product_id')[:3], 10000)
            delivery_dict_list.append({
                'delivery_no': k,
                'weight': total_weight,
                'volume': total_volume
            })

        return compose_weight_rule.filter(delivery_dict_list, weight, volume)
    else:
        return None
