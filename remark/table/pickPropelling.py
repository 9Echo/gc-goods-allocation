# -*- coding: utf-8 -*-
# Description: 摘单推荐定时项目sql说明
# Created: luck 2021/01/13


class TableRemark:
    """
    数据表说明
    """

    # 项目名称
    project_name = '摘单推荐定时项目'
    # 项目接口
    interface_name = None
    # 对表的说明
    table_remark_dict = {
        'db_ads.zd_pickup_order_driver': '初始、开始摘单、摘单中、摘单成功状态的摘单计划表',
        'db_cdm.zd_new_driver_message': '新司机表',
        'db_model.t_driver_no_interest_count': '无兴趣表',
        'db_ads.zd_plan_driver_app_gps': '通过中交兴路接口获取的二十分钟内车辆位置表',
        'db_ads.zd_hhy_driver_location': '通过APP获取的司机位置表',
        'db_cdm.dim_driver_vehicle': '司机车辆关联表',
        'db_cdm.dim_active_driver': '司机三个月内运单量表(用作筛选活跃司机)',
        'db_model.t_propelling_log': '摘单消息推送日志表'
    }


class TableSql:
    """
    数据表生成的sql语句
    """

    # 四个状态的摘单计划表
    zd_pickup_order_driver = """
        SELECT
        cast(CONCAT(po.rowid ,ifnull(pod.rowid , '')) as int ) rowid,
        po.pickup_no,
        po.start_point,
        tp.location_id end_point,
        po.total_truck_num,
        po.remain_truck_num,
        pod.driver_id,
        pod.driver_phone,
        pod.be_order_confirmed,
        tp.city_name,
        tp.district_name,
        po.create_date,
        po.pickup_time,
        po.pickup_start_time,
        po.pickup_end_time,
        po.product_name,
        po.driver_type,
        po.total_weight,
        po.remain_total_weight,
        po.pickup_status
        FROM
        db_ods.db_tender_t_pickup_order po
        LEFT JOIN db_ods.db_tender_t_pickup_order_driver pod ON po.company_id = pod.company_id AND po.pickup_no = pod.pickup_no
        LEFT JOIN db_ods.db_sys_t_point tp ON po.end_point = tp.location_id
        WHERE
        po.company_id = 'C000062070'
        AND po.pickup_status IN ('PUST00', 'PUST10', 'PUST20', 'PUST30') 
    """

    # 新司机表
    zd_new_driver_message = """
        SELECT 
        u.mobile, 
        u.name, 
        u.user_id  driver_id, 
        u.card_id,
        s.segment_id, 
        p.name city 
        FROM 
        db_ods.db_sys_t_user u 
        LEFT JOIN  (
            SELECT
            driver_id ,
            count(DISTINCT waybill_no) waybill_count 
            FROM
            db_ods.db_trans_t_waybill_driver
            GROUP BY
            driver_id
        ) d 
        ON u.user_id = d.driver_id
        LEFT JOIN  db_ods.db_sys_t_user_busi_segment s ON u.user_id = s.user_id
        LEFT JOIN (
            SELECT 
            LEFT(code,4) code,
            `name`
            FROM 
            db_ods.db_sys_t_provinces 
            WHERE  
            `level` = 'DZDJ20'
        ) p
        ON LEFT(u.card_id , 4 ) = p.code
        WHERE d.driver_id IS NULL 
        AND u.create_date >= date_sub(now() , INTERVAL 3 DAY )
        AND card_id IS NOT NULL 
        AND user_company_type = 'YHLX20'
        AND auth_status = 'RZZT90'
        AND s.segment_id ='020'
        AND LEFT(u.card_id , 2) = '37'
    """

    # 通过中交兴路接口获取的二十分钟内车辆位置表
    zd_plan_driver_app_gps = """
        SELECT
        p.plan_no,
        p.plan_status,
        pd.driver_id,
        vl.latitude,
        vl.longitude,
        vl.create_date 
        FROM
        db_trans_t_plan p
        JOIN db_trans_t_plan_driver pd ON p.plan_no = pd.plan_no 
        AND p.company_id = pd.company_id 
        AND business_moduleId = '020' 
        AND carrier_company_id = 'C000062070' 
        AND p.create_date >SUBDATE( NOW( ), INTERVAL 7 DAY ) 
        AND p.plan_status IN ( 'DDZT10', 'DDZT20', 'DDZT50', 'DDZT55', 'DDZT58', 'DDZT62', 'DDZT65', 'DDZT68', 'DDZT72', 'DDZT75', 'DDZT80', 'DDZT85' ) 
        LEFT JOIN db_trans_t_vehicle_location vl ON pd.driver_id = vl.user_id 
        AND vl.create_date > SUBDATE( NOW( ), INTERVAL 20 MINUTE )
    """

    # 通过APP获取的司机位置表
    zd_hhy_driver_location = """     
    SELECT 
    MAX(vl.rowid) rowid,
    d.driver_id, 
    d.driver_name,
    vl.latitude, 
    vl.longitude, 
    vl.location_name,
    receive_date
    FROM db_ods.db_trans_t_vehicle_location vl 
    INNER JOIN (
        SELECT
        d.driver_id,
        d.driver_name
        FROM
        db_ods.db_trans_t_waybill w
        INNER JOIN db_ods.db_trans_t_waybill_driver d ON w.waybill_no = d.waybill_no
        AND w.business_type IN ('020' , '009')
        AND w.carrier_company_id = 'C000062070'
        AND load_date >= date_sub(now() ,INTERVAL 6 MONTH ) 
        AND d.driver_id IS NOT NULL  
        GROUP BY d.driver_id 
        order BY 2
    ) d  
    ON vl.user_id = d.driver_id 
    AND receive_date >= date_sub(now() , INTERVAL 20 MINUTE)
    GROUP BY d.driver_id, vl.latitude, vl.longitude
    ORDER BY receive_date DESC  
    """

    # 司机车辆关联表
    dim_driver_vehicle = """
    SELECT
    driver_id,
    vehicle_no
    FROM
    db_ods.db_sys_t_driver_vehicle
    """

    # 司机三个月内运单量表(用作筛选活跃司机)
    dim_active_driver = """
    SELECT
    tw.driver_id,
    ubi.`name` driver_name,
    ubi.`mobile` driver_phone,
    COUNT( tw.driver_id ) waybill_count,
    tp.city_name	
    FROM
    db_ods.db_trans_t_waybill tw
    LEFT JOIN db_portrayal.user_base_info ubi ON tw.driver_id = ubi.user_id 
    LEFT JOIN db_ods.db_sys_t_point tp ON tw.end_point = tp.location_id 
    WHERE
    tp.city_name IN ( '淄博市', '菏泽市', '济南市', '滨州市' )  
    AND tw.driver_id != ''
    AND ubi.`name` != ''
    AND ubi.`mobile` != ''
    AND tp.city_name != ''
    AND tw.create_date > DATE_SUB( NOW( ), INTERVAL 3 MONTH ) 
    GROUP BY
    driver_id 
    ORDER BY
    3 DESC
    """

