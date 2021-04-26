# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/12/01

class Sql:

    # 查询4条线路单价表
    select_unit_price_by_4 = """
            SELECT DISTINCT
                city_name,
                district_name,
                product_name,
                unit_price,
                create_date 
            FROM
                ( 
                    SELECT 
                        location_id, 
                        province_name, 
                        city_name, 
                        district_name 
                    FROM 
                        db_sys_t_point 
                    WHERE 
                        city_name IN ( '菏泽市', '济南市', '淄博市', '滨州市' ) 
                ) a
                INNER JOIN ( 
                                SELECT 
                                    product_name, 
                                    end_point, 
                                    unit_price, 
                                    create_date 
                                FROM 
                                    `db_tender_t_pickup_order` 
                                WHERE 
                                    unit_price IS NOT NULL 
                            ) b 
                ON a.location_id = b.end_point 
            GROUP BY
                city_name,
                district_name,
                product_name,
                unit_price
    """
