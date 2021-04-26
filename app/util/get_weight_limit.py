# -*- coding: utf-8 -*-
# Description:
# Created: liujiaye  2020/07/10
from model_config import ModelConfig


def get_lower_limit(big_commodity_name) -> int:
    return ModelConfig.RG_COMMODITY_WEIGHT.get(big_commodity_name, ModelConfig.RG_MIN_WEIGHT)
