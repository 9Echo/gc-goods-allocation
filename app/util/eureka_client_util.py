# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/7/14 10:48
import logging
import py_eureka_client.eureka_client as eureka_client

from config import Config, get_active_config

active_config = get_active_config()


class EurekaClientUtil:
    """

    """

    @staticmethod
    def set_eureka():
        """

        :return:
        """
        try:
            eureka_client.init(eureka_server=active_config.EUREKA_SERVER,
                               app_name=Config.APP_NAME,
                               instance_port=Config.SERVER_PORT,
                               # 调用其他服务时的高可用策略，可选，默认为随机
                               ha_strategy=eureka_client.HA_STRATEGY_RANDOM)
        except Exception as e:
            logger = logging.getLogger('gunicorn.error')
            if logger:
                logger.exception(str(e))
            eureka_client.stop()
