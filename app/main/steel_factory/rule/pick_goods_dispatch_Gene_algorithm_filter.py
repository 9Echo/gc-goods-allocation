#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/3/10 10:52
# @Author  : \pingyu
# @File    : pick_goods_dispatch_Gene_algorithm_filter.py
# @Software: PyCharm
import copy
import random
from operator import itemgetter
from typing import List

import matplotlib.pyplot as plt

from app.main.steel_factory.entity.stock import Stock
from app.main.steel_factory.rule.pick_compose_public_method import can_generate_load_task


class Gene:
    def __init__(self, **data):
        self.__dict__.update(data)
        self.size = len(data['data'])


class GA:
    def __init__(self, parameter):
        """
        生成初代种群，并初始化参数
        parameter = [CXPB, MUTPB, NGEN, popsize, low, up, city_stock]
        参数分别为：交叉率，变异率，繁殖代数，种群大小，最小值，最大值，库存
        """
        self.parameter = parameter

        low = self.parameter[4]
        up = self.parameter[5]

        self.stock = self.parameter[6]
        self.n_cargo = len(self.stock)
        self.n_truck = len(self.stock)

        self.bound = []
        self.bound.append(low)
        self.bound.append(up)

        pop = []
        for i in range(self.parameter[3]):
            geneinfo = [[0 for _ in range(self.n_truck)] for _ in range(self.n_cargo)]

            for cargo_pos in range(self.n_cargo):
                truck_pos = random.randint(-1, self.n_truck - 1)
                if truck_pos == -1:
                    continue
                geneinfo[cargo_pos][truck_pos] = 1

            # evaluate each chromosome
            fitness = self.evaluate(geneinfo)
            # store the chromosome
            pop.append({'Gene': Gene(data=geneinfo), 'fitness': fitness})

        self.pop = pop
        # store the best chromosome in the population
        self.bestindividual = self.selectBest(self.pop)

    def is_compliance_with_the_rules(self, geneinfo, flag=1):
        """
        判断当前的geneinfo是否符合拼货规则
        flag=1 即默认geneinfo格式为车货状态矩阵
        flag=0 表示传入的geneinfo只是单辆车的状态
        """
        # 如果传入的单辆车状态列表，注意此时若车上无货则不符合规则
        if not flag:
            if sum(geneinfo) == 0:
                return False
            pre_cargo_list = []
            for i in range(self.n_cargo):
                if geneinfo[i]:
                    pre_cargo_list.append(self.stock[i])
            if not can_generate_load_task(pre_cargo_list):
                return False
            else:
                return True

        if sum(map(sum, geneinfo)) == 0:
            return True

        for cargo_state in geneinfo:
            if sum(cargo_state) > 1:
                return False

        for truck_pos in range(self.n_truck):
                pre_cargo_list: List[Stock] = []
                for cargo_state in range(self.n_cargo):
                    if geneinfo[cargo_state][truck_pos]:
                        pre_cargo_list.append(self.stock[cargo_state])
                if not pre_cargo_list:
                    continue
                if not can_generate_load_task(pre_cargo_list):
                    return False

        return True

    def evaluate(self, geneinfo):
        """
        若该（总）装车清单不符合业务规则则返回0
        若该（总）装车清单总重量为0则返回1
        否则返回（总）装车清单总重量
        :param geneinfo:
        :return:
        """
        if not self.is_compliance_with_the_rules(geneinfo):
            return 0

        result = 0
        for cargo_pos in range(self.n_cargo):
            for truck_pos in range(self.n_truck):
                if geneinfo[cargo_pos][truck_pos]:
                    result += self.stock[cargo_pos].actual_weight

        if result == 0:
            return 1

        return result

    def selectBest(self, pop):
        """
        初代最好的个体保留作为记录
        对整个种群按照适应度函数降序排列，取最大值的个体
        """

        s_inds = sorted(pop, key=itemgetter("fitness"), reverse=True)

        return s_inds[0]

    def selection(self, individuals, k):
        """
        # 按照概率选择后代，适应度函数大的个体大概率被选到下一代，对重新生成的新一代种群按照适应度升序排列
        """

        s_inds = sorted(individuals, key=itemgetter("fitness"), reverse=True)
        sum_fits = sum(ind['fitness'] for ind in individuals)

        chosen = []
        for i in range(k):
            # 随机生成一个[0, sum_fits]范围内的数字作为阈值
            u = random.random() * sum_fits
            sum_ = 0
            for ind in s_inds:
                sum_ += ind['fitness']
                if sum_ >= u:
                    chosen.append(ind)
                    break
        chosen = sorted(chosen, key=itemgetter("fitness"), reverse=False)
        return chosen

    def divide(self, offspring):
        """
        1. 将n份装车清单分为符合规则的子装车清单和不符合规则的子装车清单
        2. 补足两份装车清单剩余的位置（补0）
        符合规则的不变，对不符合规则的子撞车清单进行交叉/变异操作
        """
        if self.n_cargo == 1:
            return offspring

        res_gene = [[0 for _ in range(self.n_truck)] for _ in range(self.n_cargo)]
        gene = offspring.data

        reserve_gene = Gene(data=[])
        # 标记不符合规则的车次中的货物下标
        used_cargo = []
        # 标记不符合规则的车次下标
        flag_truck = []

        for truck_pos in range(self.n_truck):
            truck_state = [truck[truck_pos] for truck in gene]
            if self.is_compliance_with_the_rules(truck_state, flag=0):
                # 若该车次符合规则，存入不需要处理的res_gene中，保存该车次状态，标记当前车次上的货物状态（表示已被放置）
                for cargo in range(self.n_cargo):
                    if truck_state[cargo]:
                        used_cargo.append(cargo)
                        res_gene[cargo][truck_pos] = truck_state[cargo]
            else:
                # 如果不符合规则，存储当前车次的下标和货物下标
                flag_truck.append(truck_pos)

        # 返回未使用的货物
        not_used_cargo = [i for i in range(self.n_cargo)]
        for i in range(len(used_cargo)):
            not_used_cargo.remove(used_cargo[i])

        reserve_gene.data = res_gene

        return reserve_gene, not_used_cargo, flag_truck

    def crossoperate(self, offspring):
        """
        使用双点交叉，改变原基因序列从而实现优化
        从选中的种群中分别选出第一个和第二个子代
        """

        geninfo1 = offspring[0]['Gene'].data
        geninfo2 = offspring[1]['Gene'].data

        if self.n_cargo == 1:
            pos1 = 0
            pos2 = 0
        else:
            pos1 = random.randint(0, self.n_cargo-1)
            pos2 = random.randint(0, self.n_cargo-1)

        newoff1 = Gene(data=[])
        newoff2 = Gene(data=[])
        temp1 = []
        temp2 = []

        for cargo_index in range(self.n_cargo):
            if min(pos1, pos2) <= cargo_index < max(pos1, pos2):
                temp2.append(geninfo2[cargo_index])
                temp1.append(geninfo1[cargo_index])
            else:
                temp2.append(geninfo1[cargo_index])
                temp1.append(geninfo2[cargo_index])

        newoff1.data = temp1
        newoff2.data = temp2

        return newoff1, newoff2

    def optimization_crossoperate(self, offspring):
        """
        优化交叉过程，提高交叉后个体的成功率（个体符合规则即为成功）
        """

        geninfo1 = offspring[0]['Gene'].data
        geninfo2 = offspring[1]['Gene'].data

        newoff1 = Gene(data=[])
        newoff2 = Gene(data=[])
        temp1 = []
        temp2 = []

        newoff1.data = temp1
        newoff2.data = temp2

        return newoff1, newoff2

    def mutation(self, crossoff):
        """
        变异过程：这里使用单点变异
        """

        mut_gene = copy.deepcopy(crossoff.data)

        if self.n_cargo > 1:
            for cargo_index, cargo_state in enumerate(crossoff.data):
                pos = random.randint(0, self.n_truck - 1)
                if any(cargo_state):
                    pre_truck = crossoff.data[cargo_index].index(1)
                    mut_gene[cargo_index][pre_truck] = 0
                mut_gene[cargo_index][pos] = 1
        else:
            mut_gene[0][0] = 0 if mut_gene[0][0] else 1

        crossoff.data = mut_gene
        return crossoff

    def optimization_mutation(self, crossoff):
        """
        优化变异过程，提高变异后个体的成功率（个体符合规则即为成功）
        1. 将n份装车清单分为符合规则的子装车清单和不符合规则的子装车清单
        2. 符合规则的不变，对不符合规则的子撞车清单进行变异操作
        """
        if sum(map(sum, crossoff.data)) == 0:
            crossoff = self.mutation(crossoff)

        reserve_gene, flag_cargo, flag_truck = self.divide(crossoff)
        res_gene = reserve_gene.data
        origin_off = copy.deepcopy(crossoff)
        origin_gene = origin_off.data

        # 对原始个体进行变异操作，只针对不符合要求的车次进行变异
        for truck in flag_truck:
            if len(flag_cargo) == 0:
                break
            pos = random.choice(flag_cargo)
            origin_gene[pos][truck] = 1 if origin_gene[pos][truck] == 0 else 0
            flag_cargo.remove(pos)

        newoff1 = Gene(data=[])
        newoff2 = Gene(data=[])
        newoff1.data = res_gene
        newoff2.data = origin_gene

        return newoff1, newoff2

    def GA_main(self):
        """
        主函数入口
        利用初代种群进行进化，每一次进化都保存当前种群中最优秀的个体和对用的适应度值
        """

        CXPB = self.parameter[0]
        MUTPB = self.parameter[1]
        NGEN = self.parameter[2]
        popsize = self.parameter[3]

        best_fitness = []

        print("Start of evolution")
        # 开始进化
        for g in range(NGEN):
            print("######### Generation {} #########".format(g))

            # 根据转换的合适度选择种群
            selectpop = self.selection(self.pop, popsize)

            nextoff = []
            while selectpop:
                # 对子代进行交叉和变异
                # 选择两个个体
                offspring = [selectpop.pop() for _ in range(2)]
                if random.random() < CXPB:
                    # 进行交叉
                    crossoff1, crossoff2 = self.crossoperate(offspring)
                    if random.random() < MUTPB:
                        # 进行变异
                        muteoff1, muteoff2 = self.optimization_mutation(crossoff1)
                        muteoff3, muteoff4 = self.optimization_mutation(crossoff2)
                        fit_muteoff1 = self.evaluate(muteoff1.data)
                        fit_muteoff2 = self.evaluate(muteoff2.data)
                        fit_muteoff3 = self.evaluate(muteoff3.data)
                        fit_muteoff4 = self.evaluate(muteoff4.data)
                        nextoff.append({'Gene': muteoff1, 'fitness': fit_muteoff1})
                        nextoff.append({'Gene': muteoff2, 'fitness': fit_muteoff2})
                        nextoff.append({'Gene': muteoff3, 'fitness': fit_muteoff3})
                        nextoff.append({'Gene': muteoff4, 'fitness': fit_muteoff4})
                    else:
                        fit_crossoff1 = self.evaluate(crossoff1.data)
                        fit_crossoff2 = self.evaluate(crossoff2.data)
                        nextoff.append({'Gene': crossoff1, 'fitness': fit_crossoff1})
                        nextoff.append({'Gene': crossoff2, 'fitness': fit_crossoff2})
                else:
                    nextoff.extend(offspring)


            # 子代种群替代父代种群
            self.pop = nextoff

            # 计算并收集适应度函数
            fits = [ind['fitness'] for ind in self.pop]

            best_ind = self.selectBest(self.pop)

            if best_ind['fitness'] > self.bestindividual['fitness']:
                self.bestindividual = best_ind

            best_fitness.append(self.bestindividual['fitness'])

            print("Best individual found is {}, {}".format(self.bestindividual['Gene'].data, self.bestindividual['fitness']))

            print(" Max fitness of current pop: {}".format(max(fits)))

        print("-------- End of (successful) evolution --------")

        return self.bestindividual['Gene'].data, self.bestindividual['fitness']

        # x = [i for i in range(NGEN)]
        # y = best_fitness
        # plt.plot(x, y)
        # plt.xlabel('迭代次数')
        # plt.ylabel('总收益')
        # plt.rcParams['font.sans-serif'] = ['SimHei']
        # plt.show()
