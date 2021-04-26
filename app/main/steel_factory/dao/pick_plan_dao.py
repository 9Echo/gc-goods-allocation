# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/11/11 15:38
from app.main.steel_factory.entity.plan import Plan
from app.util.base.base_dao import BaseDao


class PickPlanDao(BaseDao):

    def get_plan(self):
        sql = """
        SELECT
        prodname,
        city_name as city,
        district_name,
        plan_quantity,
        trains_no
        FROM
        db_ads.zd_plan_open_no
        """
        data = self.select_all(sql)
        return [Plan(i) for i in data]


pick_plan_dao = PickPlanDao()
