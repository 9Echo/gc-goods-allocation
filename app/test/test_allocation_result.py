import json
import traceback

import pymysql
from json import dumps

import xlsxwriter
from pymysql import MySQLError

from app.util.db_pool import db_pool_ods
from app.main.services import order_service, dispatch_service


def get_data():
    original_data = get_data_from_table(sql)
    # print(original_data)
    org_unit_list = []

    for item in original_data:
        org_unit_list.append(item["org_unit"])
    org_unit_list = list(set(org_unit_list))
    # print(org_unit_list)

    jsondata_list = []
    for i in org_unit_list:
        data_dict ={}
        data = {"customer_id": "",
                "salesman_id": "",
                "company_id": "",
                "items": []}
        flag = False
        for j in original_data:
            data_item = {
                "product_type": "",
                "spec": "",
                "item_id": "",
                "f_whs": "sth",
                "f_loc": "sth",
                "material": "sth",
                "quantity": "",
                "free_pcs": ""
            }
            if flag == False:
                if i == j["org_unit"]:
                    flag = True
                    data["customer_id"] = j["org_unit"]
                    data["salesman_id"] = j["slsmancode"]
                    data_item["product_type"] = j["productname"]
                    data_item["item_id"] = j["itemid"]
                    data_item["quantity"] = j["order_j"]
                    data_item["free_pcs"] = j["order_g"]
                    data["items"].append(data_item)
            else:
                if i == j["org_unit"]:
                    data_item["product_type"] = j["productname"]
                    data_item["item_id"] = j["itemid"]
                    data_item["quantity"] = j["order_j"]
                    data_item["free_pcs"] = j["order_g"]
                    data["items"].append(data_item)
        data_dict["data"] = data
        jsondata_list.append(dumps(data_dict))
        # print(jsondata_list)
    # 测试代码
    # 原订单数据
    delivery_data = get_data_from_table(sql_org_unit)
    i = 0
    j = 0
    modeldata_list = []
    analysis_list = []
    for jsondata in jsondata_list:
        json_data = json.loads(jsondata)
        order = order_service.generate_order(json_data['data'])
        # print(order.as_dict())
        sheets = dispatch_service.dispatch(order)
        sheet_no = 0
        sheet_l = 0
        sheet_load = []
        for sheet in sheets:
            # print(sheet.as_dict())
            for item in sheet.items:
                i = i+1
                modeldata_list.append([sheet.load_task_id, sheet.customer_id, sheet.salesman_id, sheet.delivery_no,
                                       sheet.weight, item.quantity, item.free_pcs, item.item_id, item.material, 'model_data', i])
                sheet_no = sheet_no + 1
                if sheet.load_task_id not in sheet_load:
                    sheet_l = sheet_l + 1
                    sheet_load.append(sheet.load_task_id)
        # analysis_list.append(['model_data:',sheet.customer_id, '通知单数', sheet_no, '车次数', sheet_l])
        analysis_list.append(['model_data:',sheet.customer_id, sheet_no, sheet_l])
        delivery_no = 0
        delivery_l = 0
        delivery_load = []
        for deliverydata in delivery_data:
            if sheets[0].customer_id == deliverydata["org_unit"]:
                j = j+1
                modeldata_list.append([deliverydata["id"], deliverydata["org_unit"], deliverydata["slsmancode"], deliverydata["docuno"],
                                       deliverydata["order_cal"], deliverydata["order_j"], deliverydata["order_g"], deliverydata["itemname"],
                                       deliverydata["productname"], j])
                delivery_no = delivery_no + 1
                if deliverydata["docuno"] not in delivery_load:
                    delivery_l = delivery_l + 1
                    delivery_load.append(deliverydata["docuno"])
        # analysis_list.append(['fact_data:',sheets[0].customer_id, '通知单数', delivery_no, '车次数', delivery_l])
        analysis_list.append(['fact_data:',sheets[0].customer_id, delivery_no, delivery_l])

    # 将分析结果delivery_no、load_task_id打印出来
    # print('customer_id  delivery_no  load_task_id')
    for i in analysis_list:
        print(i)
    # 将分析结果写入excel表中
    # filename_a = 'analysisdata_list.xlsx'
    # write_excel_xls(filename_a, analysis_list)
    # filename = 'modeldata_list.xlsx'
    # write_excel_xls(filename, modeldata_list)
    # print('finish!')


def write_excel_xls(filename, data_list):
    workbook = xlsxwriter.Workbook(filename)  # 创建一个excel文件
    worksheet = workbook.add_worksheet()  # 创建一个sheet

    # 将数据写入第 i 行，第 j 列
    i = 0
    for data in data_list:
        for j in range(len(data)):
            worksheet.write(i, j, str(data[j]))
        i = i + 1
    workbook.close()


def get_data_from_table(sql):
    try:
        conn = db_pool_ods.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute(sql)
        return cursor.fetchall()
    except Exception as e:
        traceback.print_exc()
        raise MySQLError
    finally:
        cursor.close()
        conn.close()

sql = """select 
        org_unit,
        company_id,
        slsmancode,
        productname,
        itemname,
        sum(b.order_j) as order_j,
        sum(b.order_g) as order_g,
        itemid,
        GH00
        from ( 
        SELECT 
        org_unit,
        company_id,
        slsmancode,
        docuno
        FROM ods_db_inter_t_keeperhd
        where crted_date like '2019-12-19%'
        and docuno like 't%'
        and company_id ='C000000888'
        ) as a 
        left join (
        select 
        productname,
        itemname,
        order_j,
        order_g, 
        itemid,
        GH00,
        docuno
        from ods_db_inter_t_keeperln
        where crted_date like '2019-12-19%'
        ) as b
        on a.docuno = b.docuno
        group by a.org_unit,b.itemid"""

sql_org_unit = """select *
from ( 
SELECT 
id,
org_unit,
slsmancode,
docuno,
order_cal
FROM ods_db_inter_t_keeperhd
where crted_date like '2019-12-19%'
and docuno like 't%'
and company_id ='C000000888'
group by org_unit,docuno
) as a 
left join (
select 
order_j,
order_g, 
itemname,
itemid,
productname,
docuno
from ods_db_inter_t_keeperln
where crted_date like '2019-12-19%'
) as b
on a.docuno = b.docuno
"""


if __name__ == '__main__':
    get_data()
    # datas = get_data_from_table()
    # print(datas)
