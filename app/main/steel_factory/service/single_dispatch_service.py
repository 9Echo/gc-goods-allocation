# -*- coding: utf-8 -*-
# Description: 单车配载服务
# Created: shaoluyu 2020/06/16
import json
from datetime import datetime
from app.main.steel_factory.dao.load_task_dao import load_task_dao
from app.main.steel_factory.dao.load_task_item_dao import load_task_item_dao
from app.main.steel_factory.entity.load_task import LoadTask
from app.main.steel_factory.rule import single_dispatch_filter
from app.main.steel_factory.service import redis_service
from app.util.aspect.distribute_lock import distribute_lock
from app.util.generate_id import HashKey
from app.util.result import Result


@distribute_lock
def dispatch(truck):
    """
    进行单车配载
    """
    load_task = None
    load_task = single_dispatch_filter.dispatch(truck)
    if load_task:
        load_task.schedule_no = truck.schedule_no
        load_task.car_mark = truck.car_mark
        # 设置车次数
        set_load_task_count(load_task)
        # 生成的车次信息保存入库
        save_load_task(load_task)
    else:
        return Result.error('无推荐结果！')
    # 平台微服务所需格式转换
    load_task_dict = data_format(load_task)
    return Result.success(data=load_task_dict)


def data_format(load_task: LoadTask):
    """
    格式转换
    :param load_task:
    :return:
    """
    if load_task:
        dic = {
            "loadTaskId": load_task.load_task_id,  # 车次号
            "loadTaskType": load_task.load_task_type,  # 装卸类型
            "priorityGrade": load_task.priority_grade,  # 车次优先级
            "totalWeight": load_task.total_weight,  # 总重量
            "city": load_task.city,  # 城市
            "dlvSpotNameEnd": load_task.end_point,
            "earliestOrderTime": load_task.latest_order_time,
            "items": [{
                "loadTaskId": item.load_task_id,
                "priority": item.priority,
                "weight": item.weight,
                "count": item.count,
                "city": item.city,
                "dlvSpotNameEnd": item.end_point,
                "bigCommodity": item.big_commodity,
                "commodityName": item.commodity,
                "noticeNum": item.notice_num,
                "oritemNum": item.oritem_num,
                "consumer": item.consumer,
                "standard": item.standard,
                "material": item.sgsign,
                "deliware": item.instock_code,
                "deliwareHouse": item.outstock_code,
                "detailAddresss": item.receive_address,
                "latestOrderTime": item.latest_order_time
            } for item in load_task.items]
        }
        return dic
    else:
        return None


def save_load_task(load_task: LoadTask):
    load_task_values = []
    load_task_item_values = []
    create_date = datetime.now()
    # 1公司id 2计划号 3车牌号 4车次号 5装载类型 6总重量 7城市 8终点 9吨单价 10车次总价 11备注 12车次优先级 13创建人id 14创建时间
    load_task_tuple = (
        'C000000882',
        load_task.schedule_no,
        load_task.car_mark,
        load_task.load_task_id,
        load_task.load_task_type,
        load_task.total_weight,
        load_task.city,
        load_task.end_point,
        '',
        '',
        load_task.remark,
        load_task.priority_grade,
        '',
        create_date)
    load_task_values.append(load_task_tuple)
    # 1公司id 2报道号 3车次号 4优先级 5重量 6件数 7市 8终点 9大品名 10小品名 11发货通知单号
    # 12订单号 13收货用户 14规格 15材质 16出库仓库 17入库仓库 18收货地址 19最新挂单时间 20创建人id 21创建时间
    for item in load_task.items:
        item_tuple = (
            'C000000882',
            load_task.schedule_no,
            item.load_task_id,
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
            '',
            create_date)
        load_task_item_values.append(item_tuple)
    load_task_dao.insert_load_task(load_task_values, load_task)
    load_task_item_dao.insert_load_task_item(load_task_item_values, load_task)


def set_load_task_count(load_task: LoadTask):
    # 拿到old数据{hash值：{报道号：1}}
    priority_dict = redis_service.get_priority_dict()
    for load_task_item in load_task.items:
        if load_task_item.priority in ["A", "B"]:
            key = HashKey.get_key([load_task_item.notice_num, load_task_item.oritem_num, load_task_item.contract_no])
            schedule_dict = priority_dict.get(key, None)
            if schedule_dict:
                schedule_dict[load_task.schedule_no] = 1
            else:
                priority_dict[key] = {load_task.schedule_no: 1}
    redis_service.set_priority_dict(priority_dict)
