"""
存放项目中使用到的数据表的一些信息，每个项目新建一个py文件(项目接口命名)，文件中大致按照下面的结构书写
"""


class TableRemark:
    """
    数据表说明
    """

    # 项目名称
    project_name = '项目名称'
    # 项目接口
    interface_name = '项目接口'
    # 对表的说明
    table_remark_dict = {
        '库名.表名1': '说明',
        '库名.表名2': '说明',
        '库名.表名3': '说明'
    }


class TableSql:
    """
    数据表生成的sql语句
    """

    #
    table1 = """
        sql语句
    """
    #
    table2 = """
        sql语句
    """
    #
    table3 = """
        sql语句
    """
