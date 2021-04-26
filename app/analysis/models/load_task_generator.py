from app.analysis.rules import package_solution
from app.main.dao.base_dao import BaseDao
from app.util.uuid_util import UUIDUtil


class Sheet:
    def __init__(self, dn, ci, weight):
        self.delivery_no = dn
        self.customer_id = ci
        self.weight = weight


class AnalysisDao(BaseDao):

    def generate(self):
        """为重量为0或超过30吨的车生成load_task_id"""
        sql = "select delivery_no from t_ga_delivery_sheet where weight > 33000"
        rs = self.select_all(sql)
        dn_list = []
        for row in rs:
            dn_list.append(row['delivery_no'])
        update_sql = "update t_ga_delivery_sheet set load_task_id=%s where delivery_no=%s"
        values = [(UUIDUtil.create_id('lt'), dn) for dn in dn_list]
        self.executemany(update_sql, values)

    def compose(self):
        """为重量在0到30吨之间的车拼单生成load_task_id"""
        sql = "select delivery_no, customer_id ,weight from t_ga_delivery_sheet where load_task_id is NULL"
        rs = self.select_all(sql)
        csm_dict = {}
        for row in rs:
            sheet = Sheet(row['delivery_no'], row['customer_id'], row['weight'])
            csm_dict.setdefault(sheet.customer_id, []).append(sheet)
        # 根据客户进行分组，对每个客户的单子进行拼车
        for sheets in csm_dict.values():
            current = sheets
            while current:
                composed = []
                left = []
                # 候选集数量太长则截取前30个
                if len(current) > 30:
                    left = current[30:len(current)]
                    current = current[0:30]
                weight_cost = [(float(sheet.weight), float(sheet.weight)) for sheet in current]
                bestvalue, result = package_solution.dynamic_programming(len(current), 34000, weight_cost)
                if bestvalue == 0:
                    break
                print("weight:" + str(bestvalue))
                for i in range(0, len(result)):
                    if result[i] == 1:
                        composed.append(current[i])
                    else:
                        left.append(current[i])
                current = left
                for s in composed:
                    print(s.delivery_no + ":" + str(s.weight))
                update_sql = "update t_ga_delivery_sheet set load_task_id=%s where delivery_no=%s"
                load_task_id = UUIDUtil.create_id('lt')
                values = [(load_task_id, s.delivery_no) for s in composed]
                self.executemany(update_sql, values)


if __name__ == '__main__':
    dao = AnalysisDao()
    dao.generate()
    dao.compose()