from typing import List

import requests
import json

from app.util.code import ResponseCode
from app.util.my_exception import MyException
from app.util.rest_template import RestTemplate
import config
from app.main.steel_factory.entity.load_task import LoadTask
from flask import current_app

from app.util.result import Result


def service(param: Result, id_list):
    """接收状态和结果

    Args:

    Returns:

    Raise:

    """
    if param.code == ResponseCode.Success:
        data_list = list()
        for res in param.data:
            data_list.append(data_format(res, id_list))
        param.data = data_list
    url = config.get_active_config().DISPATCH_SERVICE_URL + "/truckTask/createTruckTasks"
    try:
        current_app.logger.info('进行结果反馈，url：{}'.format(url))
        result = RestTemplate.do_post(url, param.as_dict())
        current_app.logger.info(result.get('msg'))
    except MyException as me:
        current_app.logger.exception(me)
        current_app.logger.error(me.message)
    except Exception as e:
        current_app.logger.exception(e)
        current_app.logger.error('调用出错！')


def data_format(load_task: LoadTask, id_list: List[str]):
    """将load_task转化成对应接口所需的格式

    Args:

    Returns:

    Raise:

    """
    dic = {
        "companyId": id_list[0],  # 公司id
        "cargoSplitId": id_list[2],  # 分货任务号
        "loadTaskId": load_task.load_task_id,  # 车次号
        "loadTaskType": load_task.load_task_type,  # 装卸类型
        "priorityGrade": load_task.priority_grade,  # 车次优先级
        "totalWeight": load_task.total_weight,  # 总重量
        "city": load_task.city,  # 城市
        "dlvSpotNameEnd": load_task.end_point,
        "pricePerTon": load_task.price_per_ton,
        "totalPrice": load_task.total_price,
        "earliestOrderTime": load_task.latest_order_time,
        "truckTaskDetails": [{
            "loadTaskId": item.load_task_id,
            "cargoSplitId": id_list[2],
            "priority": item.priority,
            "weight": item.weight,
            "count": item.count,
            "city": item.city,
            "dlvSpotNameEnd": item.end_point,
            "bigCommodity": item.big_commodity,
            "commodity": item.commodity,
            "noticeNum": item.notice_num,
            "oritemNum": item.oritem_num,
            "consumer": item.consumer,
            "standard": item.standard,
            "material": item.sgsign,
            "noticeStockinfoId": item.notice_stockinfo_id,
            "deliware": item.instock_code,
            "deliwareHouse": item.outstock_code,
            "detailAddresss": item.receive_address,
            "latestOrderTime": item.latest_order_time
        } for item in load_task.items]
    }
    return dic
