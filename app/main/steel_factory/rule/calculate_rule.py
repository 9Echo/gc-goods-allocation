from typing import List, Any, Dict

from app.main.steel_factory.entity.load_task import LoadTask
from app.main.steel_factory.entity.stock import Stock
from app.main.steel_factory.rule.create_load_task_rule import create_load_task
from app.util.generate_id import GenerateId
from model_config import ModelConfig


def calculate(compose_list: List[Stock], general_stock_dict: Dict[int, Stock], load_task_list: List[LoadTask],
              temp_stock: Any, load_task_type: str):
    """
    重量计算
    :param compose_list:
    :param general_stock_dict:
    :param load_task_list:
    :param temp_stock:
    :param load_task_type:
    :return:
    """
    temp_dict = dict()
    # 选中的stock按照stock_id分类
    for compose_stock in compose_list:
        temp_dict.setdefault(compose_stock.stock_id, []).append(compose_stock)
    new_compose_list = list()
    if temp_stock:
        new_compose_list.append(temp_stock)
    for k, v in temp_dict.items():
        # 获取被选中的原始stock
        general_stock = general_stock_dict.get(k)
        stock = v[0]
        stock.actual_number = len(v)
        stock.actual_weight = len(v) * stock.piece_weight
        temp_weight = (general_stock.actual_number - len(v)) * general_stock.piece_weight
        # if general_stock.actual_weight>ModelConfig.RG_MIN_WEIGHT and temp_weight<ModelConfig.RG_MIN_WEIGHT:

        general_stock.actual_number -= len(v)
        general_stock.actual_weight = general_stock.actual_number * general_stock.piece_weight
        new_compose_list.append(stock)
        if general_stock.actual_number == 0:
            general_stock_dict.pop(k)
    # 生成车次数据
    load_task_list.append(
        create_load_task(new_compose_list, GenerateId.get_id(), load_task_type))
