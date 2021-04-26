# -*- coding: utf-8 -*-
# Description: 应用管理服务
# Created: shaoluyu 2019/11/13

import uuid


class UUIDUtil:
    """基于uuid1生成唯一业务号"""

    @staticmethod
    def create_id(prefix):
        return prefix + '_' + str(uuid.uuid1()).replace('-', '')[0:-12]
