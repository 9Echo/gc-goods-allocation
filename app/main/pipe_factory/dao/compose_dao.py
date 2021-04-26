from app.util.base.base_dao import BaseDao


class ComposeDao(BaseDao):
    """
    筛选可推荐的预留提货单
    """

    def get_compose_delivery(self, company_id, customer_id_list, delivery_no_list):
        """根据公司id， 客户条件，提货单号条件 查找预留提货单

        Args:

        Returns:
                data:发货单子单列表
        Raise:

        """
        sql = """
        SELECT 
        item.delivery_no,
        item.quantity,
        item.weight,
        item.product_id
        from
        t_ga_delivery_item item,
        (SELECT 
                delivery_no, 
                customer_id,
                weight
                from 
                `t_ga_delivery_sheet`
                where 
                `status` = 'FHZT00' and company_id = '{}' and customer_id in ({}) and delivery_no not in ({})) main
            where item.delivery_no = main.delivery_no
"""
        # 客户条件
        cus_values = "'"
        cus_values += "','".join([i for i in customer_id_list])
        cus_values += "'"

        # 提货单号条件
        de_values = "'"
        de_values += "','".join([i for i in delivery_no_list])
        de_values += "'"
        data = self.select_all(sql.format(company_id, cus_values, de_values))
        return data


compose_dao = ComposeDao()
