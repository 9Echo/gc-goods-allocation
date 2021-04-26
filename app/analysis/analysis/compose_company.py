from app.main.dao.company_dao import get_company_data, write_database, get_data_from_table, add_match_size, update_database_size, update_database_time
import time
import traceback


class Company:

    def analysis(self):
        try:
            # 获取t_compose_company中已有的搭配情况
            existing_data = get_data_from_table()
            # 得到更新时间
            la_time = set(existing_data['update_time'].values)
            # 如果是第一次更新
            if not la_time:
                last_time = '2019-08-01 00:00:00'
                data, now_time = get_company_data(last_time)
            else:
                # 之后的更新
                last_time = list(la_time)
                data, now_time = get_company_data(last_time[0])
                if data.empty:
                    print("没有可更新数据！")
                    return
            #  获取运单（运单号，运单create_date，车牌号）；发货通知单（发货通知单号， 理重，
            #  公司名，发货create_date）；结算单（结算单号， 实重， 结算create_date）数据

            # {waybill_no, create_date, travel_no, f_docuno, b_docuno, order_cal,
            # SJGBSL, org_unit_name, f_crted_date, j_crted_date}
            # {运单号，运单create_date，车牌号， 发货通知单号，结算单号， 理重，
            # 实重， 公司名， 发货create_date, 结算create_date }
            # print(data)

            # {车牌号:[[运单号，运单create_date，发货通知单号，结算单号，公司名，结算create_date， 理重， 实重],...]...}

            analysis_dic = {}
            for index, row in data.iterrows():
                if row['travel_no'] not in analysis_dic:
                    analysis_dic[row['travel_no']] = []
                analysis_dic[row['travel_no']].append([row['waybill_no'], row['create_date'], row['f_docuno'], row['b_docuno'],
                                                       row['org_unit_name'], row['f_crted_date'], row['order_cal'], row['SJGBSL']])
            # print(analysis_dic)

            # 判定为同一车的条件：发货通知单号、结算单号、公司名不同，
            # 两条数据的运单时间、结算单时间分别相差小于1小时
            company_list = []
            for k in analysis_dic:
                if len(analysis_dic[k]) != 1:
                    # print(k, analysis_dic[k])
                    # 为了避免重复，新建了一个列表，将每辆车已录入company_list的组合记录下来
                    comp = []
                    for i in analysis_dic[k]:
                        for j in analysis_dic[k]:
                            i1 = int(time.mktime(i[1].timetuple()))
                            j1 = int(time.mktime(j[1].timetuple()))
                            if i[2] != j[2] and i[3] != j[3] and i[4] != j[4] \
                                    and abs(i1 - j1) < 180:
                                comp1 = [i[0], j[0]]
                                comp2 = [j[0], i[0]]
                                if comp1 not in comp and comp2 not in comp:
                                    comp.append([i[0], j[0]])
                                    company_list.append([i[4], j[4]])
                                    print(k, i, 'i')
                                    print(k, j, 'j')
            if len(company_list) == 0:
                print("没有可更新数据！")

            # 添加拼车次数列
            # {('公司1', '公司2'), 拼车次数,...}
            result_dic = {}
            # 如果为第一次分析写库
            if not la_time:
                add_match_size(company_list, result_dic)
                result = sorted(result_dic.items(), key=lambda x: x[1], reverse=True)
                for re in result:
                    write_database(re, now_time)
            else:
                add_match_size(company_list, result_dic)
                # print(result_dic)
                for re in result_dic.items():
                    print(re)
                    re_e = {}
                    flag = False
                    for index, row in existing_data.iterrows():
                        # print(row)
                        row_a = (row["company_name1"], row["company_name2"])
                        row_b = (row["company_name2"], row["company_name1"])
                        if re[0] == row_a or re[0] == row_b:
                            size = re[1] + row["match_size"]
                            re_e[0] = row_a
                            re_e[1] = size
                            print(re_e)
                            update_database_size(re_e)
                            flag = True
                    if flag == False:
                        write_database(re, now_time)

            # 更新数据库时间
            update_database_time(now_time)
            # 按拼车次数给结果排序，由大到小
            # result = sorted(result_dic.items(), key=lambda x: x[1], reverse=True)

        except Exception as e:
            print("compose_company analysis error!")
            traceback.print_exc()


if __name__ == '__main__':
    com = Company()
    com.analysis()
