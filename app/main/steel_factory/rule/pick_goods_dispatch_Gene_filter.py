#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/3/10 9:19
# @Author  : \pingyu
# @File    : pick_goods_dispatch_Gene_filter.py
# @Software: PyCharm

import random
from operator import itemgetter
import matplotlib.pyplot as plt


class Gene:
    def __init__(self, **data):
        self.__dict__.update(data)
        self.size = len(data['data'])


class GA:
    def __init__(self, parameter):
        # parameter = [CXPB, MUTPB, NGEN, popsize, low, up]
        # 参数分别为：交叉率，变异率，繁殖代数，种群大小，最小值，最大值
        self.parameter = parameter

        low = self.parameter[4]
        up = self.parameter[5]

        self.bound = []
        self.bound.append(low)
        self.bound.append(up)

        pop = []
        for i in range(self.parameter[3]):
            geneinfo = []
            for pos in range(len(low)):
                # initialise population
                geneinfo.append(random.randint(self.bound[0][pos], self.bound[1][pos]))

            # evaluate each chromosome
            fitness = self.evaluate(geneinfo)
            # store the chromosome
            pop.append({'Gene': Gene(data=geneinfo), 'fitness': fitness})

        self.pop = pop
        # store the best chromosome in the population
        self.bestindividual = self.selectBest(self.pop)

    def evaluate(self, geneinfo):
        # 利用适应度函数计算函数值
        x1 = geneinfo[0]
        x2 = geneinfo[1]
        x3 = geneinfo[2]
        x4 = geneinfo[3]
        y = x1 ** 2 + x2 ** 2 + x3 ** 3 + x4 ** 4

        return y

    def selectBest(self, pop):
        # 初代最好的个体保留作为记录
        # 对整个种群按照适应度函数降序排列，取最大值的个体
        s_inds = sorted(pop, key=itemgetter("fitness"), reverse=True)

        return s_inds[0]

    def selection(self, individuals, k):
        # 按照概率选择后代，适应度函数大的个体大概率被选到下一代，对重新生成的新一代种群按照适应度升序排列
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


    def crossoperate(self, offspring):
        # 使用双点交叉，改变原基因序列从而实现优化
        dim = len(offspring[0]['Gene'].data)

        # 从选中的种群中分别选出第一个和第二个子代
        geninfo1 = offspring[0]['Gene'].data
        geninfo2 = offspring[1]['Gene'].data

        if dim == 1:
            pos1 = 1
            pos2 = 1
        else:
            pos1 = random.randrange(1, dim)
            pos2 = random.randrange(1, dim)

        newoff1 = Gene(data=[])
        newoff2 = Gene(data=[])
        temp1 = []
        temp2 = []
        for i in range(dim):
            if min(pos1, pos2) <= i < max(pos1, pos2):
                temp2.append(geninfo2[i])
                temp1.append(geninfo1[i])
            else:
                temp2.append(geninfo1[i])
                temp1.append(geninfo2[i])

        newoff1.data = temp1
        newoff2.data = temp2

        return newoff1, newoff2

    def mutation(self, crossoff, bound):
        # 变异和交叉一样，通过产生更优秀的后代实现优化，这里使用单点变异
        dim = len(crossoff.data)

        if dim == 1:
            pos = 0
        else:
            pos = random.randrange(0, dim)

        crossoff.data[pos] = random.randint(bound[0][pos], bound[1][pos])
        return crossoff

    def GA_main(self):

        popsize = self.parameter[3]
        best_fitness = []

        print("Start of evolution")
        # 开始进化
        for g in range(NGEN):
            print("######### Generation {} #########".format(g))

            # 根据转换的合适度选择种群
            selectpop = self.selection(self.pop, popsize)

            nextoff = []
            while len(nextoff) != popsize:
                # 对子代进行交叉和变异
                # 选择两个个体
                offspring = [selectpop.pop() for _ in range(2)]

                if random.random() < CXPB:
                    # 进行交叉
                    crossoff1, crossoff2 = self.crossoperate(offspring)
                    if random.random() < MUTPB:
                        # 进行变异
                        muteoff1 = self.mutation(crossoff1, self.bound)
                        muteoff2 = self.mutation(crossoff2, self.bound)
                        fit_muteoff1 = self.evaluate(muteoff1.data)
                        fit_muteoff2 = self.evaluate(muteoff2.data)
                        nextoff.append({'Gene': muteoff1, 'fitness': fit_muteoff1})
                        nextoff.append({'Gene': muteoff2, 'fitness': fit_muteoff2})
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

        # x = [i for i in range(NGEN)]
        # y = best_fitness
        # plt.plot(x, y)
        # plt.xlabel('迭代次数')
        # plt.ylabel('总收益')
        # plt.rcParams['font.sans-serif'] = ['SimHei']
        # plt.show()


if __name__ == "__main__":
    CXPB, MUTPB, NGEN, popsize = 0.8, 0.1, 1000, 100

    up = [30, 30, 30, 30]
    low = [1, 1, 1, 1]
    parameter = [CXPB, MUTPB, NGEN, popsize, low, up]
    run = GA(parameter)
    run.GA_main()
