# coding=UTF-8
import xlrd
import xlwt
import numpy as np

from app.analysis.luchengkai.entity.car_district import CarDistrict


def capacity_pool():
    # 流向对应xlsx文件
    read_book = xlrd.open_workbook(r'../static/流向对应.xlsx')
    sheet = read_book.sheet_by_name('Sheet1')
    district_dict = {}
    for row in range(sheet.nrows):
        id = int(sheet.cell(row, 0).value)
        district_name = sheet.cell(row, 1).value
        district_dict[id] = district_name

    # 流向向量集
    flow_file = '../static/liuxiang.txt'
    flow_dict = {}
    with open(flow_file, 'r') as file_to_read:
        while True:
            lines = file_to_read.readline()  # 整行读取数据
            if not lines:
                break
                pass
            p_tmp = [float(i) for i in lines.split()]
            index = district_dict[int(p_tmp[0])]
            p_tmp.pop(0)
            flow_dict[index] = p_tmp
            pass
    pass

    for value in flow_dict.values():
        sum_f = 0
        for i in range(len(value)):
            sum_f = sum_f + value[i] * value[i]
        sum_f = np.sqrt(sum_f)
        for i in range(len(value)):
            value[i] = value[i] / sum_f

    # 车辆流向关联集
    driver_file = '../static/4条线.txt'
    driver_dict = {}
    with open(driver_file, 'r', encoding='UTF-8') as file_to_read:
        while True:
            lines = file_to_read.readline()  # 整行读取数据
            if not lines:
                break
                pass
            p_tmp = [i for i in lines.split("\t")]

            # 去掉"
            tmp_car_no = [i for i in p_tmp[0].split('"')]
            car_no = tmp_car_no[1]
            tmp_district = [i for i in p_tmp[1].split('"')]
            district = tmp_district[1]
            tmp_travel_count = [i for i in p_tmp[2].split('"')]
            travel_count = int(tmp_travel_count[1])

            car_district = CarDistrict()
            car_district.district_name = district
            car_district.car_no = car_no
            car_district.travel_count = travel_count

            if car_no in driver_dict.keys():
                driver_dict[car_no].append(car_district)
            else:
                district_list = [car_district]
                driver_dict[car_no] = district_list
            pass
    pass

    # 车辆向量集
    car_vector_dict = {}
    for key in driver_dict.keys():
        num = 0.0
        vector_list = [0 for i in range(128)]
        for car_district in driver_dict[key]:
            district_name = car_district.district_name
            travel_count = car_district.travel_count
            num += travel_count
            flow_list = flow_dict[district_name]
            for i in range(len(vector_list)):
                vector_list[i] = vector_list[i] + travel_count * flow_list[i]
        # 每一维的值 / num
        for i in range(len(vector_list)):
            vector_list[i] = vector_list[i] / num
        car_vector_dict[key] = vector_list

    score_dict = {}
    for car_key in car_vector_dict.keys():
        car_district_score_list = []
        for flow_key in flow_dict.keys():
            car_vector_list = car_vector_dict[car_key]
            flow_vector_list = flow_dict[flow_key]

            car_district = CarDistrict()
            car_district.district_name = flow_key
            car_district.car_no = car_key
            score = 0.0
            for i in range(128):
                score += car_vector_list[i] * flow_vector_list[i]
            car_district.score = score
            car_district_score_list.append(car_district)
        score_dict[car_key] = car_district_score_list
        score_dict[car_key].sort()
        # score_dict[car_key] = score_dict[car_key][0:20]
    print(1)

    deal_count(score_dict)

    # save_score(score_dict)


def deal_count(score_dict):
    district_dict = {}
    for key in score_dict.keys():
        for score_item in score_dict[key]:
            if score_item.score > 0.9:
                district_name = score_item.district_name
                if district_name in district_dict.keys():
                    district_dict[district_name] += 1
                else:
                    district_dict[district_name] = 0

    book = xlwt.Workbook(encoding='utf-8', style_compression=0)
    sheet = book.add_sheet('date', cell_overwrite_ok=True)
    sheet.write(0, 0, '区县')
    sheet.write(0, 1, '车次数')
    i = 1
    for key in district_dict.keys():
        sheet.write(i, 0, key)
        sheet.write(i, 1, district_dict[key])
        i += 1

    book.save(r'../static/count_result_4.xls')


def save_score(score_dict):
    book = xlwt.Workbook(encoding='utf-8', style_compression=0)
    sheet = book.add_sheet('date', cell_overwrite_ok=True)
    sheet.write(0, 0, '车牌号')
    sheet.write(0, 1, '区县')
    sheet.write(0, 2, '分数')
    i = 0
    for key in score_dict.keys():
        j = i * 20 + 1
        sheet.write(j, 0, key)
        for score_item in score_dict[key]:
            sheet.write(j, 1, score_item.district_name)
            sheet.write(j, 2, score_item.score)
            j += 1
        i += 1

    book.save(r'../static/result.xls')


if __name__ == '__main__':
    capacity_pool()
