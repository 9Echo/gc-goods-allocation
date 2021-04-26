import copy

from app.main.pipe_factory.entity.delivery_sheet import DeliverySheet
from app.util.uuid_util import UUIDUtil
from pulp import *
from flask import g
from model_config import ModelConfig


def create_variable_list(delivery_items):
    """

    :return:
    """
    weight_list = []
    # 每件对应的体积占比
    volume_list = []
    for i in delivery_items:
        weight_list.append(i.weight)
        volume_list.append(i.volume)
    # 构建目标序列
    value_list = copy.deepcopy(weight_list)
    return weight_list, volume_list, value_list


def call_pulp_solve(weight_list, volume_list, value_list, delivery_items):
    """

    :param delivery_items:
    :param weight_list:
    :param volume_list:
    :param value_list:
    :return:
    """
    batch_no = UUIDUtil.create_id("ba")
    # 结果集
    sheets = []
    while delivery_items:
        g.LOAD_TASK_ID += 1
        # plup求解，得到选中的下标序列
        result_index_list, value = pulp_solve(weight_list, volume_list, value_list, g.MAX_WEIGHT)
        for i in sorted(result_index_list, reverse=True):
            sheet = DeliverySheet()
            item = delivery_items[i]
            sheet.items.append(item)
            # 设置提货单总体积占比
            sheet.volume = item.volume
            sheet.load_task_id = g.LOAD_TASK_ID
            sheets.append(sheet)
            weight_list.pop(i)
            volume_list.pop(i)
            value_list.pop(i)
            delivery_items.pop(i)
    return sheets


def pulp_solve(weight_list, volume_list, value_list, new_max_weight):
    capacity = new_max_weight
    r = range(len(weight_list))
    prob = LpProblem(sense=LpMaximize)
    x = [LpVariable('x%d' % i, cat=LpBinary) for i in r]  # 変数
    prob += lpDot(value_list, x)  # 目标函数
    # 约束
    prob += lpDot(weight_list, x) <= capacity
    if volume_list:
        prob += lpDot(volume_list, x) <= ModelConfig.MAX_VOLUME
    # 解题
    prob.solve()
    # print(int(value(prob.objective)))
    # print([i for i in r if value(x[i]) > 0.5])

    return [i for i in r if value(x[i]) > 0.5], int(value(prob.objective))

# 测试
# if __name__ == '__main__':
#
#     weight_list=[9,6,3,1,8,16]
#     volume_list=[0.4,0.4,0.2,0.2,0.2,0.4]
#     value_list=[9,6,3,1,8,16]
#     new_max_weight=33
#     L,weight=pulp_solve(weight_list, volume_list, value_list, new_max_weight)
#     for i in sorted(L, reverse=True):
#         print(i)
#
#     print(L,weight)
