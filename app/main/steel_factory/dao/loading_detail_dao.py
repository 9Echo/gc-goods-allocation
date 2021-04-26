# -*- coding: utf-8 -*-
# Description: 已开装车清单数据
# Created: shaoluyu 2020/06/16
from app.util.base.base_dao import BaseDao


class LoadingDetailDao(BaseDao):
    def select_loading_detail(self, truck):
        """
        查询人工已开单信息、预开单信息、模型推荐未确认信息
        :return:
        """

        sql = """
        SELECT
            schedule_no,
            notice_num,
            oritem_num,
            SUBSTRING_INDEX(outstock_name,'-',1) as outstock_name,
            weight,
            `count` 
        FROM
            db_ads.`kc_rg_valid_loading_detail` 
        WHERE 
            id like 'lms%'
        UNION ALL
        SELECT
            schedule_no,
            notice_num,
            oritem_num,
            outstock_code,
            weight,
            `count` 
        FROM
            t_load_task_item 
        WHERE
            IFNULL( schedule_no, '' ) != '' 
            AND 
            schedule_no in (SELECT DISTINCT schedule_no FROM db_ads.kc_rg_valid_loading_detail WHERE id like 'plan%')
            and schedule_no != '{}'

        """
        data = self.select_all(sql.format(truck.schedule_no))
        return data


loading_detail_dao = LoadingDetailDao()
