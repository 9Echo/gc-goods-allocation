# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/11/10

# 摘单结果数据汇总分析
# 一天的总车次数 = 人工调度派单的车次数 + 通过摘单功能派单的车次数






"""
SELECT A.*,B.*
FROM
(
SELECT
    a.plan_no,
    a.plan_source,
    a.plan_status,
    a.remark,
    a.carrier_company_name,
    a.create_date,
    b.order_no,
    b.prodname as commodity_name,
    c.province_name as province,
    c.city_name as city,
    c.district_name as district

FROM
    (
        SELECT
            plan_no,
            plan_source,
            plan_status,
            remark,
            carrier_company_name,
            create_date 
        FROM
            db_trans.`t_plan` 
        WHERE
            create_date BETWEEN '2020-11-09 00:00:00' AND '2020-11-09 23:59:59' 
            AND business_moduleId = '020' 
    ) a,
    ( 
        SELECT 
            end_point, 
            order_no, 
            plan_no, 
            prodname 
        FROM 
            db_trans.t_plan_item 
        WHERE 
            create_date BETWEEN '2020-11-09 00:00:00' AND '2020-11-09 23:59:59' 
    ) b,
    db_sys.t_point c 

WHERE
    a.plan_no = b.plan_no 
    AND a.carrier_company_name = '会好运单车' 
    AND a.plan_status NOT IN ( 'DDZT35', 'DDZT40', 'DDZT45' ) 
    AND b.end_point = c.location_id 
    AND c.city_name IN ( '菏泽市', '济南市', '淄博市', '滨州市' ) 
ORDER BY
    2
) A LEFT JOIN 
(
select d.order_no,e.source_name,e.create_date
from 
    db_tender.t_pickup_order_driver d,
    ( 
        SELECT 
            source_name, 
            create_date
        FROM 
            db_tender.t_pickup_order
        WHERE 
            create_date BETWEEN '2020-11-09 00:00:00' AND '2020-11-09 23:59:59' 
    ) e
where d.pickup_no = e.pickup_no
) B
on A.order_no = B.order_no
"""

















