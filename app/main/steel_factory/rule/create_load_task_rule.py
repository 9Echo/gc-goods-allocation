from typing import List
from app.main.steel_factory.entity.load_task import LoadTask
from app.main.steel_factory.entity.load_task_item import LoadTaskItem
from app.main.steel_factory.entity.stock import Stock
from model_config import ModelConfig


def create_load_task(stock_list: List[Stock], load_task_id, load_task_type) -> LoadTask:
    """
    车次创建方法
    :param stock_list:
    :param load_task_id:
    :param load_task_type:
    :return:
    """
    # 车次总重量
    total_weight: float = 0
    # 车次中所有优先级
    # priority_set = set()
    # 车次中所有最新挂单时间
    latest_order_time_set = set()
    # 备注列表
    remark_list = []
    # 创建车次对象
    load_task = LoadTask()
    for i in stock_list:
        load_task_item = LoadTaskItem()
        load_task_item.weight = i.actual_weight / 1000
        load_task_item.count = i.actual_number
        load_task.count += load_task_item.count     # 求一车的总件数
        load_task_item.city = i.city
        load_task.city = i.city
        load_task_item.end_point = i.dlv_spot_name_end
        load_task.end_point = i.dlv_spot_name_end
        load_task_item.pack = i.pack
        load_task_item.commodity = i.commodity_name
        load_task_item.notice_num = i.notice_num
        load_task_item.oritem_num = i.oritem_num
        load_task_item.contract_no = i.contract_no
        load_task_item.standard = i.specs
        load_task_item.sgsign = i.mark
        load_task_item.outstock_code = i.deliware_house_name
        load_task_item.instock_code = i.deliware
        load_task_item.big_commodity = i.big_commodity_name
        load_task_item.receive_address = i.detail_address
        load_task_item.parent_load_task_id = i.parent_stock_id
        load_task_item.latest_order_time = i.latest_order_time
        load_task_item.notice_stockinfo_id = i.notice_stockinfo_id
        load_task_item.load_task_id = load_task_id
        load_task_item.consumer = i.consumer
        load_task_item.can_split_weight = i.can_split_weight
        load_task_item.can_split_number = i.can_split_number
        load_task_item.source_number = i.source_number
        total_weight += load_task_item.weight
        # priority_set.add(i.priority)
        latest_order_time_set.add(load_task_item.latest_order_time)
        remark_list.extend(ModelConfig.RG_VARIETY_VEHICLE.get(load_task_item.big_commodity, []))
        load_task_item.priority = ModelConfig.RG_PRIORITY_GRADE[i.priority]
        load_task.items.append(load_task_item)
    remark_set = set(remark_list)
    load_task.load_task_id = load_task_id
    load_task.total_weight = round(total_weight, 3)
    load_task.load_task_type = load_task_type
    # load_task.priority_grade = ModelConfig.RG_PRIORITY_GRADE[min(priority_set)]
    load_task.latest_order_time = min(latest_order_time_set)
    load_task.remark = ",".join(remark_set)
    return load_task
