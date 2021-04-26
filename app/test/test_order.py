from app.util.db_pool import db_pool_ods
from app.main.services import order_service, dispatch_service
import csv


def test():
    try:
        # 查主子表数据
        sql = """
            select t1.docuno,t1.org_unit,t2.order_j, t2.itemid,t2.productname,t2.order_g,t1.order_zg00,t1.order_cal, t2.productname
            from ods_db_inter_t_keeperhd t1, ods_db_inter_t_keeperln t2
            where t1.docuno like "t%" and t1.id= t2.main_id and t1.crted_date like "2019-12-19%" and t1.data_address = "0030"
        """
        conn = db_pool_ods.connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        result = {}
        # 封装数据
        for d in data:
            if d[1] not in result:
                result[d[1]] = {}
            if d[0] not in result[d[1]].keys():
                result[d[1]][d[0]] = {}
            if d[3] not in result[d[1]][d[0]]:
                result[d[1]][d[0]][d[3]] = []
            result[d[1]][d[0]][d[3]].append([d[4], d[2], d[5], d[6], d[7], d[8]])
        # 处理封装数据
        result2 = {}
        for key in result:
            result2[key] = {}
            dt1 = result[key]
            for i in dt1:
                result2[key][i] = {}
                dt2 = dt1[i]
                for j in dt2:
                    dt3 = dt2[j]
                    result2[key][i][j] = [dt3[0][0], int(dt3[0][1]) if dt3[0][1] else 0, int(dt3[0][2]) if dt3[0][2] else 0]
                    for q in range(len(dt3)):
                        if q > 0:
                            result2[key][i][j][1] += int(dt3[q][1]) if dt3[q][1] else 0
                            result2[key][i][j][2] += int(dt3[q][2]) if dt3[q][2] else 0
        return result, result2
    except:
        print("错误")
    finally:
        conn.close()


if __name__ == "__main__":
    a, b = test()
    # print(a)
    # print(b)
    f = open("data.csv", "w")
    csv_write = csv.writer(f)
    csv_write.writerow(["customer_id", "total_pcs", "weight", "load_task_id", "delivery_no", "product_type", "item_id", "weight2", "quantity", "free_pcs", "total_pcs"])
    for i in b:
        dt = b[i]
        data = {
            "customer_id": i,
            "salesman_id": "",
            "company_id": "",
            "items": []
        }
        # 记录主单个数
        count_old = 0
        # 记录详单数
        count_old_x = 0
        for j in dt:
            count_old += 1
            dt2 = dt[j]
            for p in dt2:
                # 得到和当前item_id相同的源数据库数据（一天中同一个item_id的一个或多个放货通知单）
                dt_a = a[i][j][p]
                # 得到当前item_id的总的[品名，总件数，散根数]
                dt_b = b[i][j][p]
                # 遍历dt_a 将当前item_id的发货通知单写入文件
                for z in dt_a:
                    count_old_x += 1
                    csv_write.writerow([i, "暂无", "暂无", "暂无", "暂无", z[5], p, z[4], z[1], z[2], z[3]])
                # 传入的数据格式
                item = {
                    "product_type": dt_b[0],
                    "spec": "",
                    "item_id": p,
                    "f_whs": "",
                    "f_loc": "",
                    "material": "",
                    "quantity": dt_b[1],
                    "free_pcs": dt_b[2]
                }
                # 添加到data中的items列表中
                data["items"].append(item)
        # 通过这两个方法生成发货通知单。
        csv_write.writerow(["主单个数", count_old, "详单数", count_old_x])
        order = order_service.generate_order(data)
        sheets = dispatch_service.dispatch(order)
        # 存放生成的load_task_id
        temp_list = []
        # 记录生成的发货通知单的load_task_id数
        count_new = 0
        # 遍历生成的发货通知单
        for q in sheets:
            # 记录不同的车次id
            if q.load_task_id not in temp_list:
                temp_list.append(q.load_task_id)
            # 遍历发货通知单中的子单
            for h in q.items:
                count_new += 1
                # 写入文件 公司缩写，主表总根数，主表重量，子表发货通知到，子表物资代码，子表重量，子表件数，子表散根数，子表总根数
                csv_write.writerow([q.customer_id, q.total_pcs, q.weight, q.load_task_id, h.delivery_no, h.product_type, h.item_id, h.weight, h.quantity, h.free_pcs, q.total_pcs])
        csv_write.writerow(["去重的车次数", len(temp_list), "生成的总单数", count_new])
    f.close()








