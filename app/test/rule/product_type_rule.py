# -*- coding: utf-8 -*-
# @Time    : 2019/11/11 17:19
# @Author  : Zihao.Liu

# 将产品品类分组，属于一个group的可以分到同一单，其余的每个品类各一单
similar_groups = [('热镀', '热度', '热镀1'), ('焊管', '焊管 ', '焊管1')]
similar = {"热镀": "热镀",
           "热度": "热镀",
           "热镀1": "热镀",
           "焊管": "焊管",
           "焊管 ": "焊管",
           "焊管1": "焊管",
           "焊管2": "焊管",
           "焊管3": "焊管",
           }


def filter(delivery_items: list):
    """
    产品类型过滤规则
    """
    filtered_items = []
    target_group = None
    for item in delivery_items:
        for i in range(0, len(similar_groups)):
            if similar_groups[i].__contains__(item.product_type):
                target_group = similar_groups[i]
                break
        if target_group:
            break
    if target_group is None:
        # 订单中没有属于分组内的产品时,首选第一条数据，将跟第一条数据品种一样的数据筛选出来
        product_type = delivery_items[0].product_type
        for i in delivery_items:
            if i.product_type == product_type:
                filtered_items.append(i)
        # filtered_items.append(delivery_items[0])
    else:
        for item in delivery_items[:]:
            if target_group.__contains__(item.product_type):
                filtered_items.append(item)
    return filtered_items


def get_product_type(sheet):
    """输出产品品类，如果属于同一品类组则返回第一个品类"""
    # product_type = sheet.product_type
    # for group in similar_groups:
    #     if group.__contains__(product_type):
    #         product_type = group[0]
    # return product_type
    return similar[sheet.product_type] if sheet.product_type in similar else sheet.product_type


