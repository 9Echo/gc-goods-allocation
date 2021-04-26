# -*- coding: utf-8 -*-
# Description: 车次主表
# Created: shaoluyu 2020/06/16
from app.main.steel_factory.entity.load_task import LoadTask
from app.util.base.base_dao import BaseDao


class LoadTaskDao(BaseDao):
    """
    LoadTask相关数据库操作
    """

    def insert_load_task(self, values, load_task: LoadTask):
        """增
        Args:
        Returns:
        Raise:
        """
        sql_list = []
        # 重复报道时，清除相同报道号的历史记录
        delete_sql = """
                        delete 
                        from t_load_task 
                        where schedule_no = '{}'
                    """
        sql_list.append(delete_sql.format(load_task.schedule_no))
        # 1公司id 2计划号 3车牌号 4车次号 5装载类型 6总重量 7城市 8终点 9吨单价 10车次总价 11备注 12车次优先级 13创建人id 14创建时间
        insert_sql = """
                        insert into t_load_task(
                                            company_id,
                                            schedule_no,
                                            car_mark,
                                            load_task_id,
                                            load_task_type,
                                            total_weight,
                                            city,
                                            end_point,
                                            price_per_ton,
                                            total_price,
                                            remark,
                                            priority_grade,
                                            create_id,
                                            `create_date`
                                    ) 
                        value(%s, %s, %s, %s, %s, 
                              %s, %s, %s, %s, %s, 
                              %s, %s, %s, %s
                              )
                    """
        sql_list.append(insert_sql)
        self.execute_many_sql(sql_list, values)


load_task_dao = LoadTaskDao()
