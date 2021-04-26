from typing import List

from app.main.steel_factory.dao.pick_plan_dao import pick_plan_dao
from app.main.steel_factory.entity.pick_task import PickTask
from app.main.steel_factory.entity.plan import Plan
from app.util.round_util import round_util


def data_format(pick_list: List[PickTask], plan_list: List[Plan]):
    """
    格式转换
    :param pick_list:
    :param plan_list:
    :return:
    """
    # 未开单对应的调度单列表
    no_plan_pick_list = []
    # 未开单有效调度单
    # plan_list: List[Plan] = pick_plan_dao.get_plan()

    for plan_item in plan_list:
        for pick in pick_list:
            no_plan_pick_list.append(weight_delete(plan_item, pick))

            if not no_plan_pick_list:
                same_truck_plan_list = [plan for plan in plan_list if plan.trains_no == plan_item.trains_no and
                                        (plan.district_name != plan_item.district_name or
                                         plan.prodname != plan_item.prodname or
                                         plan.plan_quantity != plan_item.plan_quantity
                                         )]

                # 跨厂区，一辆车上存在多条plan
                if same_truck_plan_list:
                    same_truck_plan = same_truck_plan_list[0]
                    for same_pick in pick_list:
                        no_plan_pick_list.append(weight_delete(same_truck_plan, same_pick))

    return no_plan_pick_list


def weight_delete(plan: Plan, pick: PickTask):
    no_plan = []
    temp_pick_item_list = [pick_item for pick_item in pick.items if
                           pick_item.end_point == plan.district_name and
                           pick_item.big_commodity == plan.prodname and
                           pick_item.truck_count == plan.plan_quantity]
    if not temp_pick_item_list:
        temp_pick_item_list = [pick_item for pick_item in pick.items if
                               pick_item.end_point == plan.district_name and
                               pick_item.big_commodity == plan.prodname]
    if temp_pick_item_list:
        temp_pick_item = temp_pick_item_list[0]
        no_plan.append(temp_pick_item)
        # 平均一车重量
        weight = temp_pick_item.total_weight / temp_pick_item.truck_num
        # 摘单计划明细车次数减一
        temp_pick_item.truck_num -= 1
        # 重新计算重量
        temp_pick_item.total_weight = round_util(weight * temp_pick_item.truck_num)
        # 摘单计划车次数减一
        pick.truck_num -= 1
        # 重新计算摘单计划重量
        pick.total_weight -= weight

    return no_plan


if __name__ == '__main__':
    test_pick_list = []
    test_plan_list = []
    for i in range(10):
        item = PickTask()

    for i in range(10):
        item = Plan()

    result = data_format(test_pick_list, test_plan_list)

