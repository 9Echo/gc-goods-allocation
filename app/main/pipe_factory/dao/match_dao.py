# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/8/2 15:45
from app.util.base.base_dao import BaseDao


class MatchDao(BaseDao):
    def select_match_data(self, order, max_delivery_items, min_delivery_items):
        sql = """
            select
            distinct t1.main_item_id,t1.sub_item_id,t1.city_name,t1.main_quantity,t1.sub_quantity,t1.combine_count
            from
            t_gc_history_match t1,
            (select
            a.main_item_id,
            a.sub_item_id,
            a.city_name,
            max(a.combine_count) as combine_count
            from
            t_gc_history_match a
            where
             a.main_item_id in ({}) 
            and (a.sub_item_id in ({}) or ifnull(a.sub_item_id,'') = '')
            GROUP BY a.main_item_id,a.sub_item_id,a.city_name) t2 
            where t1.main_item_id = t2.main_item_id 
            and t1.sub_item_id = t2.sub_item_id
            and t1.city_name = t2.city_name
            and t1.combine_count = t2.combine_count	
            order by t1.main_item_id,t1.combine_count desc
        """
        # ,(SELECT city_name FROM db_sys.`t_company_address` where company_id = '{}') b
        # a.city_name = b.city_name
        max_item_id_values = "'"
        max_item_id_values += "','".join([i.item_id for i in max_delivery_items])
        max_item_id_values += "'"
        min_item_id_values = "'"
        min_item_id_values += "','".join([i.item_id for i in min_delivery_items])
        min_item_id_values += "'"
        return self.select_all(sql.format(max_item_id_values, min_item_id_values))


match_dao = MatchDao()
