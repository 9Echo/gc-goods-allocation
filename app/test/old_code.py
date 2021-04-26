# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/7/16 13:47

# delivery_item = DeliveryItem()
            # delivery_item.product_type = item.product_type
            # delivery_item.spec = item.spec
            # delivery_item.quantity = item.quantity
            # delivery_item.free_pcs = item.free_pcs
            # delivery_item.item_id = item.item_id
            # delivery_item.material = item.material
            # delivery_item.f_whs = item.f_whs
            # delivery_item.f_loc = item.f_loc


# result_dict = dict()
            # for item in sheets_1:
            #     for i in item.items:
            #         result_dict.setdefault(i.item_id, []).append(i.quantity)
            # for k, v in result_dict.items():
            #     print(k + '件数' + str(sum(v)))


# # -*- coding: utf-8 -*-
# # Description:
# # Created: liujiaye  2019/06/27
# # Last-Modified: liujiaye 2019/08/29
#
# from app.user.data.load_task import LoadTask
# from app.user.data.car import Car
# from app.user.tool.can_collocate import distinguish_by_trans_group
# from app.user.control.goods_maintain import goods_management
# import pandas as pd
# from app.user.analysis.collocation_analysis import commodity_collocation_dic
# import app.user.database.allocation_db as allocation_db
# import traceback
# from app.user.data.stock import Stocks
# import copy
# from flask import current_app
# from app.user.tool.can_collocate import can_collocate
#
# loading_task_code = 0  # 车次号
#
#
# class Allot:
#
#     def __init__(self, car):
#         '''
#         构造函数
#         :param car: 车辆信息json
#         '''
#         try:
#             self.car = Car()
#             self.load_task = LoadTask()
#             self.company_id = ''
#             # 根据车辆信息中的车队，找出该车队以往的运送信息 格式如下：
#             # type:[[city,district,commodity],[city,district,commodity],...]
#             self.filter_list_all = []
#             # 可发品种，车队可发的所有品种  格式如下：
#             # type:[commodity1, commodity2,...]
#             self.__can_send_commodity_list = []
#             # 车队可发且 可拼 的品种名列表
#             self.__can_collocation_commodity_list = []
#             # 数据预处理
#             self.preprocessing(car)
#             # 初始化或更新 self.__can_send_commodity_list
#             self.update_collocation_commodity_list()
#             # self.main_commodity
#         except Exception as e:
#             print("function __init__ error")
#             traceback.print_exc()
#             current_app.logger.info("allot init error")
#             current_app.logger.exception(e)
#
#     def preprocessing(self, car):
#         """
#         数据预处理，初始化数据成员
#         初始化了self.filter_list_all、__can_send_commodity_list
#         :param car:  车辆信息 json
#         :return:
#         """
#         try:
#             # 将传入的车辆信息json转换为DataFrame
#             car_pd = pd.DataFrame(car, index=[0])
#             for index, row in car_pd.iterrows():
#                 # 初始化车辆信息
#                 self.car.init(row)
#                 if 'requestCompanyId' in row:
#                     self.company_id = row['requestCompanyId']
#             # load_task init
#             self.load_task.init(self.car)
#             # 根据车队信息得到可以得到该车队的所有城市、区域、品种——所有筛选条件
#             # tmp_filter_list_all 格式：[[city, district, prod_name],...]
#             tmp_filter_list_all = distinguish_by_trans_group(self.car.trans_group_name)
#             # 没有车队的情况  就把当前车辆信息中的城市、区、大品名 添加到self.filter_list_all 当做筛选条件
#             if tmp_filter_list_all is []:
#                 self.filter_list_all.append([self.car.city, self.car.district, self.car.commodity])
#             # 初始化self.filter_list_all
#             # 找出其中 和 车辆信息的城市相同的条件  添加到self.filter_list_all中
#             for i in tmp_filter_list_all:
#                 # i的格式:[city,district,commodity]
#                 if self.car.city == i[0]:
#                     self.filter_list_all.append(i)
#             # 初始化 __can_send_commodity_list
#             # 获取所有筛选条件中所有可发的品种 添加到__can_send_commodity_list
#             for line in self.filter_list_all:
#                 if line[2] not in self.__can_send_commodity_list:
#                     self.__can_send_commodity_list.append(line[2])
#             # self.load_task.load_limit = load_capacity_recommand(self.car.commodity)  # 获取该车辆装货的重量上下限{low:0.0,high:0.0}
#             # self.main_commodity = copy.deepcopy(self.car.commodity)
#         except Exception as e:
#             print("function preprocessing error")
#             traceback.print_exc()
#             current_app.logger.info("allot preprocessing error")
#             current_app.logger.exception(e)
#
#     def allot(self):
#         '''
#         分货功能：分出推荐车次（goods_list-->load_task），标记推荐车次，数据写库维护
#         :return: self.load_task  type：LoadTask  推荐车次
#         '''
#         try:
#             # 调用实时货物信息维护类，“筛选条件”作为参数，获得货物列表
#             filter_list = []
#             # 根据 可拼且可发的品名列表和车辆信息中的品名  在车队可发的信息self.filter_list_all中找出相关信息添加到filter_list中
#             for line in self.filter_list_all:
#                 if line[2] in self.__can_collocation_commodity_list or line[2] == self.car.commodity:
#                     filter_list.append(line)
#             # 得到满足filter_list 的 货物信息
#             goods_list = goods_management.first_filter(filter_list)
#             # 从goods_list中生成多个装车清单  从拿取一个最优的赋给self.load_task
#             self.allot_rule_opt(goods_list)
#             goods_management.add_my_status(self.load_task)  # 推荐分货结果增加标记
#             self.database_write()  # 写数据库
#             return self.load_task
#         except Exception as e:
#             traceback.print_exc()
#             current_app.logger.info("allot function error")
#             current_app.logger.exception(e)
#             return LoadTask()
#
#     def allot_rule_opt(self, goods_list):
#         '''
#         goods_list-->load_task
#         1.筛选货物
#         2.按照货物优先级，顺序提取同地址的货物，调用分货算法，获取一个装车清单，加入候选集
#         3.提取同仓库的货物，调用分货算法，获取一个装车清单，加入候选集
#         4.在装车清单候选集中选取一个装车清单作为最终结果
#         :return:load_task type:LoadTask 一个装车清单
#         '''
#         try:
#             # 车次候选集
#             load_task_list = []
#             # 货物筛选类，创建时按照优先级排序
#             goods_filter = GoodsFilter(goods_list)
#             # init
#             need_loop = True
#             index = 0
#             # 创建空的车次对象
#             load_task = LoadTask()
#             load_task.init(self.car)
#             # 已经遍历过的仓库和地址不再遍历，减少循环次数
#             traversal_addresses = []
#             traversal_stocks = []
#
#             while need_loop and index < len(goods_filter.goods_list):  # 循环遍历货物
#                 need_loop = False
#                 if goods_filter.goods_list[index].commodity_big != self.car.commodity:
#                     # 不允许换货的情况
#                     need_loop = True
#                     index += 1
#                     continue
#                 # 得到货物的卸货地址
#                 tmp_address = goods_filter.goods_list[index].unloading_address
#                 # 根据当前最高优先级的货物卸货地址筛选相同卸货地址的货物
#                 if tmp_address not in traversal_addresses:
#                     # 添加已经 操作过的 卸货地址，
#                     traversal_addresses.append(tmp_address)
#                     # 得到一个仓库集对象   其中存放着与  tmp_address(卸货地址)相同的货物  的出库仓库
#                     stocks = Stocks(goods_filter.get_goods_by_address(tmp_address))
#                     load_task = self.get_goods_from_stocks(stocks)
#                     load_task_list.append(load_task)
#                 # 得到货物的出库仓库
#                 tmp_stock = goods_filter.goods_list[index].out_stock
#
#                 if load_task.load < self.load_task.load_limit['low']:
#                     # 按照当前最高优先级的货物所在仓库选取同个仓库的所有货物
#                     if tmp_stock in traversal_stocks:
#                         need_loop = True
#                     else:
#                         # 增加仓库遍历标记
#                         traversal_stocks.append(tmp_stock)
#                         stocks = Stocks(goods_filter.get_goods_by_stocks(tmp_stock))
#                         load_task = self.get_goods_from_stocks(stocks)
#                         load_task_list.append(load_task)
#                         if load_task.load < self.load_task.load_limit['low']:
#                             need_loop = True
#                 index += 1
#             if need_loop:
#                 # 从候选集中择优
#                 best_load_task = LoadTask()
#                 best_load_task.init(self.car)
#                 # 重量最大车次
#                 for i in load_task_list:
#                     if i.load > best_load_task.load:
#                         best_load_task = i
#                         self.load_task = copy.deepcopy(best_load_task)
#             else:
#                 self.load_task = copy.deepcopy(load_task)
#         except Exception as e:
#             print("function allot_rule_opt error")
#             traceback.print_exc()
#             current_app.logger.info("allot rule opt error")
#
#     def get_goods_from_stocks(self, stocks):
#         '''
#         货物按照仓库分组后，提取一个装车清单,stocks-->load_task
#         :param stocks: 仓库列表
#         :param load_task: 装车清单对象
#         :return:
#         '''
#         return self.get_goods_greed(stocks)
#
#     def get_goods_dp(self):
#         '''
#         使用动态规划方法获取货物形成一个装车清单
#         :return:
#         '''
#         pass
#
#     def get_goods_greed(self, stocks):
#         '''
#         使用贪心算法获取货物形成一个装车清单
#         :param stocks:仓库集对象
#         :return:load_task:最大载重的装车清单或是  已满的装车清单
#         '''
#         try:
#             # 得到stocks中优先级最高的仓库中优先级最高的订单对应的货物列表
#             # 格式为：[[g1,g2,...],...]  g为Goods对象 同一个订单内的货物是一种货物的单位量，即g1、g2  相同
#             order_list = stocks.get_order_list()
#             load_task_list = []  # 车次候选集
#
#             def swap_positions(original_list, i):
#                 """
#                 将传入的列表下标为i的元素和下标为0的元素交换
#                 :param original_list:
#                 :param i:
#                 :return: copy_list:交换后的列表
#                 """
#                 copy_list = copy.deepcopy(original_list)
#                 copy_list[0], copy_list[i] = copy_list[i], copy_list[0]
#                 return copy_list
#
#             index = 0
#             # 调整 order_list 中货物的顺序 得到同一辆车的不同的装车清单加入候选集
#             while index < len(order_list):
#                 # 创建空的车次对象
#                 load_task = LoadTask()
#                 load_task.init(self.car)
#                 # 调整顺序
#                 sort_order_list = swap_positions(order_list, index)
#                 for order in sort_order_list:
#                     for g in order:
#                         # 满足 can_collocate中的条件后添加货物并得到 mark：表示此次添加是否成功、或添加后清单是否已满
#                         if can_collocate(g, load_task):
#                             mark = load_task.add_goods(g)
#                             if mark == -1:
#                                 break
#                             if mark == 0:
#                                 return load_task
#                 index += 1
#                 load_task_list.append(load_task)
#             # 载重量最大的清单
#             biggest_load_task = LoadTask()
#             # 找出最大载重的清单
#             for i in load_task_list:
#                 if i.load > biggest_load_task.load:
#                     biggest_load_task = i
#             return biggest_load_task
#         except Exception as e:
#             print("function get_goods_greed error")
#             traceback.print_exc()
#             current_app.logger.info("allot get_goods_greed error")
#
#     def update_collocation_commodity_list(self):
#         '''
#         第一次初始化 __can_collocation_commodity_list
#         后续更新可拼货的品种list（可拼或且在可发列表中的货物品种）
#         :return:
#         '''
#         try:
#             # commodity_collocation_dic格式：{main_commodity: Collocation对象,...}
#             # 即 {白卷： Collocation对象, 窄带： Collocation对象,...}
#             # Collocation对象中self.match_key的格式：{match_key:match_size}
#             # 以白卷为例即 {窄带：24, 黑卷：78,...}
#             if self.car.commodity in commodity_collocation_dic:
#                 # 得到可以与车辆信息中品名搭配的  品名
#                 collocation_commodity_list = commodity_collocation_dic[self.car.commodity].match_key.keys()
#                 # 通过和车队可发的品名列表比较  得到可发且可搭配的品名列表
#                 for i in self.__can_send_commodity_list:
#                     if i in collocation_commodity_list:
#                         self.__can_collocation_commodity_list.append(i)
#         except Exception as e:
#             print("function update_collocation_commodity_list error")
#             traceback.print_exc()
#             current_app.logger.info("function update_collocation_commodity_list error")
#             current_app.logger.exception(e)
#
#     def database_write(self):
#         try:
#             #将分货结果写到数据库
#             allocation_db.write_allocation_result(self.company_id, self.load_task)
#         except Exception as e:
#             print("function database_write error")
#             traceback.print_exc()
#             current_app.logger.info("function database_write error")
#             current_app.logger.exception(e)
#
#
# class GoodsFilter:
#     '按照需求管理货物list'
#
#     def __init__(self, goods_list):
#         self.goods_list = goods_list
#         self.__sort_by_mark()
#
#     def __sort_by_mark(self):
#         self.goods_list.sort(key=lambda x: x.mark, reverse=True)
#
#     def get_goods_by_stocks(self, stocks):
#         """
#         将self.goods_list中出库仓库与stocks相同的货物找出来
#         :param stocks: 出库仓库
#         :return: filtered_goods_list:符合条件的货物列表
#         """
#         filtered_goods_list = []
#         for i in self.goods_list:
#             if i.out_stock == stocks:
#                 filtered_goods_list.append(i)
#         return filtered_goods_list
#
#     def get_goods_by_address(self, address):
#         """
#         将self.goods_list中地址与address相同的货物找出来
#         :param address: 卸货地址
#         :return: filtered_goods_list:符合条件的货物列表
#         """
#         filtered_goods_list = []
#         for i in self.goods_list:
#             if i.unloading_address == address:
#                 filtered_goods_list.append(i)
#         return filtered_goods_list
print(int(0.00009))