from pulp import *


def package(weight_list, volume_list, value_list):
    capacity = 33000
    r = range(len(weight_list))
    prob = LpProblem(sense=LpMaximize)
    x = [LpVariable('x%d' % i, cat=LpBinary) for i in r]  # 変数
    prob += lpDot(value_list, x)  # 目标函数
    # 约束
    prob += lpDot(weight_list, x) <= capacity
    # m += lpDot(volume_list, x) >= 1.17
    prob += lpDot(volume_list, x) <= 1.18
    # 解题
    prob.solve()
    print(('车次载重'+ str(value(prob.objective))+'kg', '选中变量'+str([i for i in r if value(x[i]) > 0.5])))
    # print(len(result_index_list))
    return [i for i in r if value(x[i]) > 0.5]
