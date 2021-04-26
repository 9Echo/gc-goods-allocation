# -*- coding: utf-8 -*-
# @Time    : 2020/01/19
# @Author  : shaoluyu
import math

from scipy.optimize import minimize

from app.util import weight_calculator
import numpy as np


def my_minimize(delivery_items):
    max_quantity = []
    one_weight = []
    order_j = []
    max_weight = 34
    max_volume = 1.18
    total_weight = 0
    # 计算总重量、最大件数、单件重量、件数信息
    for i in delivery_items:
        # 总重量
        total_weight += i.weight
        max_quantity.append(i.max_quantity if i.max_quantity else 10000)
        one_weight.append(weight_calculator.calculate_weight(i.product_type, i.item_id, 1, 0) / 1000)
        order_j.append(i.quantity)
    # 车次等于总重量除以最大载重量
    car_count = math.ceil(total_weight / 1000 / max_weight)
    # 品种规格数量
    product_type_count = len(delivery_items)
    # 矩阵元素个数等于品种数*车次数
    count = car_count * product_type_count
    # 初始化矩阵
    x0 = np.zeros(30)
    args = (max_quantity, one_weight, order_j, max_weight, max_volume, car_count, product_type_count)
    # 添加约束
    cons = con(args)
    bnds = ()
    for i in range(count):
        bnds += ((0, None),)
    # 非线性规划求最优解
    res = minimize(fun(args), x0, method='SLSQP', bounds=bnds, constraints=cons)
    print(res.fun)
    print(res.success)
    # print(res.x)
    print(res.x.reshape(car_count, product_type_count))


def con(args):
    # 约束条件 分为eq 和ineq
    # eq表示 函数结果等于0 ； ineq 表示 表达式大于等于0
    max_quantity, one_weight, order_j, max_weight, max_volume, car_count, product_type_count = args
    cons = ()
    # 添加件数被分配完的约束
    for i in range(product_type_count):
        cons += ({'type': 'eq',
                      'fun': lambda x: sum([x[product_type_count * j + i] for j in range(car_count)]) - order_j[i]},)
    # 添加车次总重量不超过最大载重的约束
    for i in range(car_count):
        cons += ({'type': 'ineq', 'fun': lambda x: max_weight - sum(
            [x[j + product_type_count * i] * one_weight[j] for j in range(product_type_count)])},)
    # 添加车次总体积不超过最大体积占比
    for i in range(car_count):
        cons += ({'type': 'ineq', 'fun': lambda x: max_volume - sum(
            [x[j + product_type_count * i] * (1 / max_quantity[j]) for j in range(product_type_count)])},)

    return cons


def fun(args):
    max_quantity, one_weight, order_j, max_weight, max_volume, car_count, product_type_count = args

    # 目标函数，残差平方和最小
    def my_method(x):
        y = 0
        for i in range(car_count):
            temp = 0
            for j in range(product_type_count):
                print(j + product_type_count * i)
                temp += x[j + product_type_count * i] * one_weight[j]
            y += (max_weight - temp) ** 2
        return y

    return my_method
