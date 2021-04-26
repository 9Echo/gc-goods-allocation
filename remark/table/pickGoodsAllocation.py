# -*- coding: utf-8 -*-
# Description: 摘单分货项目数据表说明
# Created: jjunf 2021/01/12


class TableRemark:
    """
    数据表说明
    """

    # 项目名称
    project_name = '摘单分货项目'
    # 项目接口
    interface_name = '/pickGoodsAllocation'
    # 对表的说明
    table_remark_dict = {
        'db_ads.zd_plan_open_no': '摘单未开单调度单表，在最后返回摘单计划时需要扣除此表中的车次',
        'db_model.t_pick_log': '摘单分货日志表',
        'db_model.t_pick_deduct_log': '摘单分货需要扣除的车次记录表'
    }


class TableSql:
    """
    数据表生成的sql语句
    """

    # 摘单未开单调度单表
    zd_plan_open_no = """
        SELECT
            a.driver,
            a.plan_no,
            a.trains_no,
            a.plan_source,
            a.remark,
            b.prodname,
            b.plan_weight,
            b.plan_quantity,
            c.city_name,
            c.district_name 
        FROM
            (
                select
                    t2.driver,
                    t2.plan_no,
                    t2.trains_no,
                    t2.plan_source,
                    t2.remark
                from
                (
                    SELECT 
                        distinct trains_no
                    FROM 
                        db_ods.db_trans_t_plan 
                    WHERE 
                        business_moduleId = '020' 
                        AND carrier_company_id = 'C000062070' 
                        AND plan_status IN ( 'DDZT10', 'DDZT20', 'DDZT30', 'DDZT50' )
                ) t1 
                join db_ods.db_trans_t_plan t2 on t1.trains_no = t2.trains_no 
            ) a
            JOIN db_ods.db_trans_t_plan_item b ON a.plan_no = b.plan_no
            JOIN db_ods.db_sys_t_point c ON b.end_point = c.location_id 
        WHERE
            city_name IN ( '济南市', '滨州市', '菏泽市', '淄博市' ) 
    """
