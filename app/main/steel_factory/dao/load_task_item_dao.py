# -*- coding: utf-8 -*-
# Description: 车次明细表
# Created: shaoluyu 2020/06/16
from app.main.steel_factory.entity.load_task import LoadTask
from app.util.base.base_dao import BaseDao


class LoadTaskItemDao(BaseDao):
    """
    LoadTaskItem相关数据库操作
    """

    def insert_load_task_item(self, values, load_task: LoadTask):
        """增
        Args:
        Returns:
        Raise:
        """
        sql_list = []
        # 重复报道时，清除历史记录
        delete_sql = """
                        delete 
                        from t_load_task_item 
                        where schedule_no = '{}'
                    """
        sql_list.append(delete_sql.format(load_task.schedule_no))
        # 1公司id 2车次号 3优先级 4重量 5件数 6城市 7终点 8大品名 9小品名 10发货通知单号
        # 11订单号 12收货用户 13规格 14材质 15出库仓库 16入库仓库 17收货地址 18最新挂单时间 19创建人id 20创建时间
        insert_sql = """
                        insert into t_load_task_item(
                                            company_id,
                                            schedule_no,
                                            load_task_id,
                                            priority,
                                            weight,
                                            `count`,
                                            city,
                                            end_point,
                                            big_commodity,
                                            commodity,
                                            notice_num,
                                            oritem_num,
                                            consumer,
                                            standard,
                                            sgsign,
                                            outstock_code,
                                            instock_code,
                                            receive_address,
                                            latest_order_time,
                                            create_id,
                                            `create_date`
                                    )
                        value(%s, %s, %s, %s, %s, 
                               %s, %s, %s, %s, %s, 
                               %s, %s, %s, %s, %s, 
                               %s, %s, %s, %s, %s,
                               %s)
                    """
        sql_list.append(insert_sql)
        self.execute_many_sql(sql_list, values)


load_task_item_dao = LoadTaskItemDao()
