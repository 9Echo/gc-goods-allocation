from typing import List, Dict, Any

from app.analysis.luchengkai.entity.actual_trans import ActualTrans
from app.analysis.luchengkai.entity.free_time import FreeTime
from collections import defaultdict


def calculate_free_time(data_dict: Dict[str, List[ActualTrans]]):
    free_time_dict = defaultdict(list)
    for key in data_dict.keys():
        temp_actual_list = data_dict[key]
        flag = False
        for temp_actual in temp_actual_list:
            if temp_actual.spd == 0 and flag is False:
                free_time = FreeTime()
                free_time.stop_date = temp_actual.create_date
                flag = True
            elif temp_actual.spd != 0 and flag is True:
                free_time.start_date = temp_actual.create_date
                free_time_dict[key].append(free_time)
                flag = False

    free_start_count_dict = {}
    free_stop_count_dict = {}
    for key in free_time_dict.keys():
        temp_free_list = free_time_dict[key]
        for temp_free in temp_free_list:
            if temp_free.start_date not in free_start_count_dict:
                free_start_count_dict[temp_free.start_date] = 1
            else:
                free_start_count_dict[temp_free.start_date] += 1

            if temp_free.stop_date not in free_stop_count_dict:
                free_stop_count_dict[temp_free.stop_date] = 1
            else:
                free_stop_count_dict[temp_free.stop_date] += 1

    return_stop_dict = {}
    return_start_dict = {}
    dict_key = sorted(free_stop_count_dict.keys())
    for key in dict_key:
        return_stop_dict[key] = free_stop_count_dict[key]

    dict_key = sorted(free_start_count_dict.keys())
    for key in dict_key:
        return_start_dict[key] = free_start_count_dict[key]

    for key in return_stop_dict.keys():
        if key not in return_start_dict:
            return_start_dict[key] = 0
    return return_start_dict, return_stop_dict

