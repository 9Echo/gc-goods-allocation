from app.util.db_pool import db_pool_ods

#
#
conn = db_pool_ods.connection()
# sql = """
#         SELECT DISTINCT
#             t1.customer_id,
#             t1.delivery_no,
#             t2.product_id,
#             t2.material
#
#         FROM
#             `ods_db_trans_plan_t_ga_delivery_sheet` t1,
#             `ods_db_trans_plan_t_ga_delivery_item` t2
#         WHERE
#             t1.delivery_no = t2.delivery_no
#         order by t2.product_id
#         -- group by t2.product_id
#     """
sql = """
        SELECT distinct 
            t1.org_unit_name,
            t2.itemid,
            t2.GH00,
            date_format(t1.crted_date, '%Y-%m-%d') as dat
        FROM
            ods_db_inter_t_keeperhd t1,
            ods_db_inter_t_keeperln t2 
        WHERE
            t1.company_id = 'C000000888' 
            AND SUBDATE( CURDATE( ), INTERVAL 30 DAY ) <= t1.crted_date 
            AND t1.id = t2.main_id
            and t1.org_unit_name is not null
        order by dat
    """
data = {}
cursor = conn.cursor()
cursor.execute(sql)
res = cursor.fetchall()
last = {}
temp = {}
result = {}
for re in res:
    data.setdefault(re[0], {}).setdefault(re[3], set()).add((re[1], re[2]))
for i in data:
    length = len(data[i])
    if length == 1:
        continue
    for j in data[i]:
        temp[i] = temp.setdefault(i, set()) | data[i][j]
        last.setdefault(i, []).extend(list(data[i][j]))
    last[i].append(length)
for k in temp:
    list1 = last[k]
    length = list1[-1]
    set1 = temp[k]
    for t in set1:
        result.setdefault(k, {}).setdefault(t, "{},{}".format(list1.count(t), length))
for n in result:
    for m in result[n]:
        d = result[n][m].split(",")
        if int(d[0])/float(d[1]) > 0.5:
            print("{},{},{}".format(n, m, result[n][m]))

print(result)

# import pandas as pd
#
# df1 = pd.DataFrame([['a', 10, '男'],
#                     ['b', 11, '男'],
#                     ['c', 11, '女'],
#                     ['a', 10, '女'],
#                     ['c', 11, '男']],
#                    columns=['name', 'age', 'sex'])
#
# df2 = pd.DataFrame([['a', 10, '男']],
#                    columns=['name', 'age', 'sex'])
# df1 = df1.append(df2)
# df1 = df1.drop_duplicates(keep=False)
# df1["a"] = False
# if not df1.loc[1]["a"]:
#     print("A")
# print(type(df1.loc[1]["a"]))
# s1 = {1,1,2}
# s2 = {3,1,4}
# print(s1)
# lit = [1, 2, 2, 23, 4, 5, 6]
# lit.extend(list(s1))
# print(lit.count(1))
