from pulp import *
from flask import g
from model_config import ModelConfig
import sys


# sys.setrecursionlimit(3000)  # 设置最大递归深度为3000


def pulp_pack(weight_list, volume_list, value_list, new_max_weight):
    capacity = new_max_weight or g.MAX_WEIGHT
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
    # print(len([i for i in r if value(x[i]) > 0.5]))
    # print(len(result_index_list))
    return [i for i in r if value(x[i]) > 0.5], value(prob.objective)


if __name__ == '__main__':
    a, b = pulp_pack([411, 411, 411, 9307, 9307, 9307, 9307, 9824, 9824, 9824], None,
                     [411, 411, 411, 9307, 9307, 9307, 9307, 9824, 9824, 9824], 22676)
    print(a, b)
