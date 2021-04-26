# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/10/24 14:29
from app.main.steel_factory.rule.pick_create_load_task_rule import create_load_task
from app.util.enum_util import LoadTaskType
from app.util.generate_id import GenerateId
from model_config import ModelConfig


def j_handler(surplus_stock_list, load_task_list):
    new_surplus_stock_list = []
    for i in surplus_stock_list:
        # 卷类1件，并且重量大于24t可发走
        if (i.big_commodity_name in ModelConfig.RG_J_GROUP and i.actual_number == 1
                and i.actual_weight >= ModelConfig.RG_SECOND_MIN_WEIGHT):
            load_task_list.append(create_load_task([i], GenerateId.get_id(), LoadTaskType.TYPE_1.value))
        else:
            new_surplus_stock_list.append(i)
    return new_surplus_stock_list
