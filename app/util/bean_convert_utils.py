# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/7/16 13:16
from typing import Any


class BeanConvertUtils:
    """

    """

    @staticmethod
    def copy_properties(obj: Any, target_type: Any):
        bean = None
        if obj and target_type:
            bean = target_type()
            for attr in bean.__dict__.keys():
                value = getattr(obj, attr, None)
                setattr(bean, attr, value)
        return bean
