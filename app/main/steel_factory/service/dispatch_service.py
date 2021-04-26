# -*- coding: utf-8 -*-
# Description: 钢铁配货服务
# Created: shaoluyu 2020/03/12
from typing import List
from app.main.steel_factory.entity.load_task import LoadTask
from app.main.steel_factory.rule import tail_stock_grouping_rule
from app.main.steel_factory.rule.dispatch_filter import dispatch_filter, create_load_task
from app.util.enum_util import LoadTaskType
from datetime import datetime
from app.main.steel_factory.dao.load_task_dao import load_task_dao
from app.main.steel_factory.dao.load_task_item_dao import load_task_item_dao
from app.util.generate_id import GenerateId


def dispatch(stock_list, sift_stock_list) -> List[LoadTask]:
    """
    车辆配货
    :param :
    :return:
    """
    load_task_list = list()
    # 重置车次id
    GenerateId.set_id()
    # 货物按尾货-tail、锁货-lock、大批量货-huge分组
    stock_dic = tail_stock_grouping_rule.tail_grouping_filter(stock_list)
    surplus_stock_dict = dispatch_filter(load_task_list, stock_dic)
    # 分不到标载车次的部分，甩掉，生成一个伪车次加明细
    if surplus_stock_dict or sift_stock_list:
        sift_stock_list.extend(list(surplus_stock_dict.values()))
        load_task_list.append(
            create_load_task(sift_stock_list, GenerateId.get_surplus_id(),
                             LoadTaskType.TYPE_5.value))
    # 合并
    merge_result(load_task_list)
    return load_task_list


def merge_result(load_task_list: list):
    """合并结果中load_task_id相同的信息

    Args:
        load_task_list: load_task的列表
    Returns:

    Raise:

    """

    for load_task in load_task_list:
        result_dic = {}
        for item in load_task.items:
            # 按（车次ID，车次父ID）整理车次
            result_dic.setdefault(item.parent_load_task_id, []).append(item)
        # 暂时清空items
        load_task.items = []
        for res_list in result_dic.values():
            sum_list = [(i.weight, i.count) for i in res_list]
            sum_weight = sum(i[0] for i in sum_list)
            sum_count = sum(i[1] for i in sum_list)
            res_list[0].weight = round(sum_weight, 3)
            res_list[0].count = sum_count
            load_task.items.append(res_list[0])
        del result_dic


def save_load_task(load_task_list: List[LoadTask], id_list):
    """将load_task对象的信息写入数据库
    分load_task和load_task_item
    Args:

    Returns:

    Raise:

    """
    load_task_values = []
    load_task_item_values = []
    create_date = datetime.now()
    # 1公司id 2计划号 3车牌号 4车次号 5装载类型 6总重量 7城市 8终点 9吨单价 10车次总价 11备注 12车次优先级 13创建人id 14创建时间
    for task in load_task_list:
        task_tup = (id_list[0],
                    '',
                    '',
                    task.load_task_id,
                    task.load_task_type,
                    task.total_weight,
                    task.city,
                    task.end_point,
                    task.price_per_ton,
                    task.total_price,
                    task.remark,
                    task.priority_grade,
                    id_list[1],
                    create_date
                    )
        load_task_values.append(task_tup)
        # 1公司id 2报道号 3车次号 4优先级 5重量 6件数 7市 8终点 9大品名 10小品名 11发货通知单号
        # 12订单号 13收货用户 14规格 15材质 16出库仓库 17入库仓库 18收货地址 19最新挂单时间 20创建人id 21创建时间
        for item in task.items:
            item_tup = (id_list[0],
                        '',
                        task.load_task_id,
                        item.priority,
                        item.weight,
                        item.count,
                        item.city,
                        item.end_point,
                        item.big_commodity,
                        item.commodity,
                        item.notice_num,
                        item.oritem_num,
                        item.consumer,
                        item.standard,
                        item.sgsign,
                        item.outstock_code,
                        item.instock_code,
                        item.receive_address,
                        item.latest_order_time,
                        id_list[1],
                        create_date)
            load_task_item_values.append(item_tup)
    load_task_dao.insert_load_task(load_task_values)  # insert_load_task中参数发生变化
    load_task_item_dao.insert_load_task_item(load_task_item_values)  # insert_load_task中参数发生变化

# if __name__ == '__main__':
#     print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '程序开始')
#     result = dispatch(["C000000882", "ct"])
#     print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '程序结束')
