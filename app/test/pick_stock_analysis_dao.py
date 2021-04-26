# -*- coding: utf-8 -*-
# Description: 库存查询
# Created: jjunf 2020/09/28
from app.util.base.base_dao import BaseDao


class StockDao(BaseDao):

    def select_plan_stock(self):
        """
        查询运营找车的调度表信息
        :return:
        """
        sql = """
                SELECT 
                    CONCAT_WS(';',company_id,plan_no) as schedule_no,	
                    plan_weight,
                    remark
                FROM 
                    db_trans.`t_plan`
                WHERE 
                    plan_status >= 'DDZT55'
                    AND business_moduleId = '020'
                    AND create_date >= '2020-07-01 00:00:00'
                    AND plan_weight >= 29 
        """
        data = self.select_all(sql)
        return data


    def select_lms_loading_main_stock(self):
        """
        lms装车清单主表,得到主清单号
        :return:
        """
        sql = """
                SELECT 
                    main_product_list_no as main_no,	
                    schedule_no
                FROM 
                    db_inter.`lms_bclp_loading_main`
                WHERE 
                    create_date >= '2020-07-01 00:00:00'
        """
        data = self.select_all(sql)
        return data


    def select_lms_loading_detail_stock(self):
        """
        lms装车清单详情表，查找同一主清单号的货物的所有重量（西门开单重量）
        :return:
        """
        sql = """
                SELECT 
                    main_prod_list_no as main_no,	
                    commodity_name,
                    outstock_code,
                    instock_code,
                    outstock_name,
                    instock_name,
                    sum(weight) AS xmkd_weight
                FROM 
                    db_inter.`lms_bclp_loading_detail`
                WHERE 
                    create_date >= '2020-07-01 00:00:00'
                GROUP BY 
                    main_prod_list_no
        """
        data = self.select_all(sql)
        return data


    def select_out_stock_weight(self):
        """
        出库清单详情表，
        :return:
        """
        sql = """
                SELECT 
                    main_product_list_no as main_no,
                    detail_address,
                    sum(weight) AS real_weight
                FROM 
                    db_inter.`lms_bclp_stacking_info_detail`
                WHERE 
                    create_date >= '2020-07-01 00:00:00'
                GROUP BY 
                    main_product_list_no
        """
        data = self.select_all(sql)
        return data


stock_analysis_dao = StockDao()
