#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/4/1 10:01
# @Author  : \pingyu
# @File    : pick_goods_dispatch_MOGA_filter.py
# @Software: PyCharm

# -*- coding: utf-8 -*-
import numpy as np
import geatpy as ea


class MyProblem(ea.Problem): # 继承Problem父类
    def __init__(self):
        name = 'MyProblem' # 初始化name（函数名称，可以随意设置）
        # 定义需要匹配的句子
        M = 2 # 初始化M（目标维数）
        maxormins = [1] * M # 初始化maxormins（目标最小最大化标记列表，1：最小化该目标；-1：最大化该目标）
        Dim = 2 # 初始化Dim（决策变量维数）
        varTypes = [0] * Dim # 初始化varTypes（决策变量的类型，元素为0表示对应的变量是连续的；1表示是离散的）
        lb = [-5] * Dim # 决策变量下界
        ub = [5] * Dim # 决策变量上界
        # 上下边界表示开区间（1）、闭区间（0）
        lbin = [1] * Dim # 决策变量下边界
        ubin = [1] * Dim # 决策变量上边界
        # 调用父类构造方法完成实例化
        ea.Problem.__init__(self, name, M, maxormins, Dim, varTypes, lb, ub, lbin, ubin)

    def aimFunc(self, pop): # 目标函数
        x1 = pop.Phen[:, [0]]
        x2 = pop.Phen[:, [1]]
        pop.ObjV = np.zeros((pop.Phen.shape[0], self.M))
        pop.ObjV[:, [0]] = x1**4-10*x1**2+x1*x2+x2**4-x1**2*x2**2
        pop.ObjV[:, [1]] = x2**4-x1**2*x2**2+x1**4+x1*x2


if __name__ == "__main__":
    """================================实例化问题对象============================="""
    problem = MyProblem()     # 生成问题对象
    """==================================种群设置================================"""
    Encoding = 'RI'           # 编码方式
    NIND = 1000               # 种群规模
    Field = ea.crtfld(Encoding, problem.varTypes, problem.ranges, problem.borders) # 创建区域描述器
    population = ea.Population(Encoding, Field, NIND) # 实例化种群对象（此时种群还没被初始化，仅仅是完成种群对象的实例化）

    """
    这里可以插入”循环尝试初始化可行染色体“的代码
    """
    """=================================算法参数设置=============================="""
    myAlgorithm = ea.moea_NSGA2_templet(problem, population) # 实例化一个算法模板对象
    myAlgorithm.MAXGEN = 100  # 最大进化代数
    myAlgorithm.logTras = 0  # 设置每多少代记录日志，若设置成0则表示不记录日志
    myAlgorithm.verbose = False  # 设置是否打印输出日志信息
    myAlgorithm.drawing = 1  # 设置绘图方式（0：不绘图；1：绘制结果图；2：绘制目标空间过程动画；3：绘制决策空间过程动画）
    """==========================调用算法模板进行种群进化=========================
    调用run执行算法模板，得到帕累托最优解集NDSet以及最后一代种群。NDSet是一个种群类Population的对象。
    NDSet.ObjV为最优解个体的目标函数值；NDSet.Phen为对应的决策变量值。
    详见Population.py中关于种群类的定义。
    """
    [NDSet, population] = myAlgorithm.run()  # 执行算法模板，得到非支配种群以及最后一代种群
    NDSet.save()  # 把非支配种群的信息保存到文件中
    """==================================输出结果=============================="""
    print('用时：%s 秒' % myAlgorithm.passTime)
    print('非支配个体数：%d 个' % NDSet.sizes) if NDSet.sizes != 0 else print('没有找到可行解！')