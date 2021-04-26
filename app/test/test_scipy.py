# -*- coding: utf-8 -*-
# @Time    : 2020/01/16
# @Author  : shaoluyu

from scipy.optimize import minimize
import numpy as np

from app.util import collection_util

"""

计算  (2+x1)/(1+x2) - 3*x1+4*x3 的最小值  x1,x2,x3的范围都在0.1到0.9 之间

"""
volume = [1000, 34, 22, 22, 34]
one_weight = [1.067, .751, 1.488, .641, .815]
order_j = [50, 40, 40, 40, 30]
max_weight = 33
max_volume = 1.18


def fun(args):
    volume, one_weight, order_j, max_weight, max_volume = args
    v = lambda x: (max_weight - collection_util.dot(x[:5], one_weight)) ** 2 + \
                  (max_weight - collection_util.dot(x[5:10], one_weight)) ** 2 \
                  + (max_weight - collection_util.dot(x[10:15], one_weight)) ** 2 \
                  + (max_weight - collection_util.dot(x[15:20], one_weight)) ** 2 \
                  + (max_weight - collection_util.dot(x[20:25], one_weight)) ** 2 \
                  + (max_weight - collection_util.dot(x[25:30], one_weight)) ** 2
    return v


def con(args):
    # 约束条件 分为eq 和ineq
    # eq表示 函数结果等于0 ； ineq 表示 表达式大于等于0
    volume, one_weight, order_j, max_weight, max_volume = args
    cons = (
        # 每个规格的总件数等于所有车次加起来一共的总件数
        {'type': 'eq',
         'fun': lambda x: order_j[0] - (x[0] + x[5 + 0] + x[5 + 5 + 0] + x[5 + 5 + 5 + 0] + x[5 + 5 + 5 + 5 + 0] + x[
             5 + 5 + 5 + 5 + 5 + 0])}, \
        {'type': 'eq',
         'fun': lambda x: order_j[1] - (x[1] + x[5 + 1] + x[5 + 5 + 1] + x[5 + 5 + 5 + 1] + x[5 + 5 + 5 + 5 + 1] + x[
             5 + 5 + 5 + 5 + 5 + 1])}, \
        {'type': 'eq',
         'fun': lambda x: order_j[2] - (x[2] + x[5 + 2] + x[5 + 5 + 2] + x[5 + 5 + 5 + 2] + x[5 + 5 + 5 + 5 + 2] + x[
             5 + 5 + 5 + 5 + 5 + 2])}, \
        {'type': 'eq',
         'fun': lambda x: order_j[3] - (x[3] + x[5 + 3] + x[5 + 5 + 3] + x[5 + 5 + 5 + 3] + x[5 + 5 + 5 + 5 + 3] + x[
             5 + 5 + 5 + 5 + 5 + 3])}, \
        {'type': 'eq',
         'fun': lambda x: order_j[4] - (x[4] + x[5 + 4] + x[5 + 5 + 4] + x[5 + 5 + 5 + 4] + x[5 + 5 + 5 + 5 + 4] + x[
             5 + 5 + 5 + 5 + 5 + 4])}, \
        # 每辆车的载重小于最大载重
        {'type': 'ineq',
         'fun': lambda x: max_weight - (
                 x[0] * one_weight[0] + x[1] * one_weight[1] + x[2] * one_weight[2] +
                 x[3] * one_weight[3] + x[4] * one_weight[4])}, \
        {'type': 'ineq',
         'fun': lambda x: max_weight - (
                 x[0 + 5] * one_weight[0] + x[0 + 5 + 1] * one_weight[1] + x[0 + 5 + 2] * one_weight[2] +
                 x[0 + 5 + 3] * one_weight[3] + x[0 + 5 + 4] * one_weight[4])}, \
        {'type': 'ineq',
         'fun': lambda x: max_weight - (
                 x[0 + 2 * 5] * one_weight[0] + x[0 + 2 * 5 + 1] * one_weight[1] + x[0 + 2 * 5 + 2] * one_weight[2] +
                 x[0 + 2 * 5 + 3] * one_weight[3] + x[0 + 2 * 5 + 4] * one_weight[4])},
        {'type': 'ineq',
         'fun': lambda x: max_weight - (
                 x[0 + 3 * 5] * one_weight[0] + x[0 + 3 * 5 + 1] * one_weight[1] + x[0 + 3 * 5 + 2] * one_weight[2] +
                 x[0 + 3 * 5 + 3] * one_weight[3] + x[0 + 3 * 5 + 4] * one_weight[4])},
        {'type': 'ineq',
         'fun': lambda x: max_weight - (
                 x[0 + 4 * 5] * one_weight[0] + x[0 + 4 * 5 + 1] * one_weight[1] + x[0 + 4 * 5 + 1] * one_weight[2] +
                 x[0 + 4 * 5 + 1] * one_weight[3] + x[0 + 4 * 5 + 1] * one_weight[4])}, \
        {'type': 'ineq',
         'fun': lambda x: max_weight - (
                 x[0 + 5 * 5] * one_weight[0] + x[0 + 5 * 5 + 1] * one_weight[1] + x[0 + 5 * 5 + 2] * one_weight[2] +
                 x[0 + 5 * 5 + 3] * one_weight[3] + x[0 + 5 * 5 + 4] * one_weight[4])}, \
        # 每辆车的体积占比小于最大体积占比
        {'type': 'ineq',
         'fun': lambda x: max_volume - (
                 x[0] * (1 / volume[0]) + x[1] * (1 / volume[1]) + x[2] * (1 / volume[2]) +
                 x[3] * (1 / volume[3]) + x[4] * (1 / volume[4]))}, \
        {'type': 'ineq',
         'fun': lambda x: max_volume - (
                 x[0 + 5] * (1 / volume[0]) + x[0 + 5 + 1] * (1 / volume[1]) + x[0 + 5 + 2] * (1 / volume[2]) +
                 x[0 + 5 + 3] * (1 / volume[3]) + x[0 + 5 + 4] * (1 / volume[4]))}, \
        {'type': 'ineq',
         'fun': lambda x: max_volume - (
                 x[0 + 2 * 5] * (1 / volume[0]) + x[0 + 2 * 5 + 1] * (1 / volume[1]) + x[0 + 2 * 5 + 2] * (
                 1 / volume[2]) +
                 x[0 + 2 * 5 + 3] * (1 / volume[3]) + x[0 + 2 * 5 + 4] * (1 / volume[4]))},
        {'type': 'ineq',
         'fun': lambda x: max_volume - (
                 x[0 + 3 * 5] * (1 / volume[0]) + x[0 + 3 * 5 + 1] * (1 / volume[1]) + x[0 + 3 * 5 + 2] * (
                 1 / volume[2]) +
                 x[0 + 3 * 5 + 3] * (1 / volume[3]) + x[0 + 3 * 5 + 4] * (1 / volume[4]))},
        {'type': 'ineq',
         'fun': lambda x: max_volume - (
                 x[0 + 4 * 5] * (1 / volume[0]) + x[0 + 4 * 5 + 1] * (1 / volume[1]) + x[0 + 4 * 5 + 1] * (
                 1 / volume[2]) +
                 x[0 + 4 * 5 + 1] * (1 / volume[3]) + x[0 + 4 * 5 + 1] * (1 / volume[4]))}, \
        {'type': 'ineq',
         'fun': lambda x: max_volume - (
                 x[0 + 5 * 5] * (1 / volume[0]) + x[0 + 5 * 5 + 1] * (1 / volume[1]) + x[0 + 5 * 5 + 2] * (
                 1 / volume[2]) +
                 x[0 + 5 * 5 + 3] * (1 / volume[3]) + x[0 + 5 * 5 + 4] * (1 / volume[4]))}, \
        # 约束矩阵所有点为整数
        # {'type': 'eq', 'fun': lambda x: x % 1}
    )
    cons += ({'type': 'ineq', 'fun': lambda x: sum([item % 1 for item in x])},)
    return cons


if __name__ == "__main__":
    # 定义常量值
    # args = (2, 1, 3, 4)  # a,b,c,d
    # 设置参数范围/约束条件
    args = (volume, one_weight, order_j, max_weight, max_volume)  # x1min, x1max, x2min, x2max
    cons = con(args)
    # 设置初始猜测值   6*5
    x0 = np.zeros(30)
    # x0 = np.asarray((0, 0, 0))
    bnds = ()
    for i in range(0, 30):
        bnds += ((0, 50),)

    res = minimize(fun(args), x0, method='SLSQP', bounds=bnds, constraints=cons, options={'maxiter': 100000})
    print(res.fun)
    print(res.success)
    print(res.message)
    print(res.nit)
    # print(res.x)
    result = res.x.reshape(6, 5)
    print(result)
    print(result.sum(axis=0))
    result = np.around(result)
    print(result)
    for i in range(6):
        print('第' + str(i + 1) + '车载重              体积占比')
        print(result[i, 0] * one_weight[0] + result[i, 1] * one_weight[1]
              + result[i, 2] * one_weight[2] + result[i, 3] * one_weight[3] + result[i, 4] * one_weight[4],
              result[i, 0] * 1 / volume[0] + result[i, 1] * 1 / volume[1]
              + result[i, 2] * 1 / volume[2] + result[i, 3] * 1 / volume[3] + result[i, 4] * 1 / volume[4])
    print(result.sum(axis=0))
