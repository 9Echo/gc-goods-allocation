import time
from datetime import datetime
from typing import List, Dict

from app.analysis.luchengkai.entity.actual_trans import ActualTrans
from collections import defaultdict


def swap_df_to_list(data_list, str_type):
    actual_time_list: List[ActualTrans] = []
    for data in data_list:
        actual_time = ActualTrans()
        actual_time.waybill_no = data[0]
        actual_time.car_mark = data[1]
        actual_time.longitude = data[2]
        actual_time.latitude = data[3]
        if str_type == 'free_time_quantum':
            str_data = str(data[4]).split(":")
        else:
            str_data = str(data[4]).split(" ")[1].split(":")
        actual_time.create_date = str_data[0]
        actual_time.plan_no = data[5]
        actual_time.adr = data[6]
        actual_time.country = data[8]
        actual_time.drc = data[9]
        actual_time.spd = data[10]

        actual_time_list.append(actual_time)
    return actual_time_list


def split_trans_group(data_list, attr1) -> Dict[str, List[ActualTrans]]:
    """
        将data_list按照attr1属性分组
        :param attr1:
        :param data_list:
        :return:
        """
    # 结果字典：{‘attr1’：[车辆轨迹列表]}
    train_dict = defaultdict(list)
    for data in data_list:
        key = getattr(data, attr1)
        train_dict[key].append(data)
    return train_dict

