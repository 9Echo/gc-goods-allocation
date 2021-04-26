import xlrd

from app.util.base import base_dao

excel_file = 'static/stock1-3.xls'


def clean_data():
    dao = base_dao.BaseDao()
    dao.execute("truncate table db_ads.kc_rg_product_can_be_send_amount")
    dao.execute("truncate table db_ads.kc_rg_valid_loading_detail")
    dao.execute("truncate table db_model.t_load_task")
    dao.execute("truncate table db_model.t_load_task_item")
    print('数据清理完毕')


def import_data(excel_list):
    sql_temp = "INSERT INTO db_ads.kc_rg_product_can_be_send_amount %KEYS% VALUES %VALUES%"
    for elem in excel_list:
        keys = ''
        for key in elem.keys():
            keys += str(key) + ','
        keys = '(' + keys[0:len(keys)-1] + ')'
        values = ''
        for value in elem.values():
            if value:
                values += '\'' + str(value) + '\'' + ','
            else:
                values += 'null' + ','
        values = '(' + values[0:len(values)-1] + ')'
        sql = sql_temp.replace('%KEYS%', keys).replace('%VALUES%', values)
        print(sql)
        dao = base_dao.BaseDao()
        dao.execute(sql)


def read_excel():
    # 打开excel表，填写路径
    book = xlrd.open_workbook(excel_file)
    # 找到sheet页
    table = book.sheet_by_name("Sheet1")
    # 获取总行数总列数
    row_Num = table.nrows
    col_Num = table.ncols
    list = []
    key = table.row_values(0)  # 这是第一行数据，作为字典的key值

    if row_Num <= 1:
        print("没数据")
        return list
    for i in range(1, row_Num):
        row_dict = {}
        values = table.row_values(i)
        for x in range(col_Num):
            # 把key值对应的value赋值给key，每行循环
            row_dict[key[x]] = values[x]
        # 把字典加到列表中
        list.append(row_dict)
    return list


if __name__ == '__main__':
    # excel_list = read_excel()
    # import_data(excel_list)
    clean_data()