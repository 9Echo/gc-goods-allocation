import xlrd


def deal():
    # 流向对应xlsx文件
    read_book = xlrd.open_workbook(r'../static/流向对应.xlsx')
    sheet = read_book.sheet_by_name('Sheet1')
    district_dict = {}
    for row in range(sheet.nrows):
        id = int(sheet.cell(row, 0).value)  # 获取i行3列的表格值
        district_name = sheet.cell(row, 1).value  # 获取i行4列的表格值
        district_dict[id] = district_name

    # 流向向量集
    flow_file = '../static/liuxiang.txt'
    flow_dict = {}
    with open(flow_file, 'r') as file_to_read:
        while True:
            sum = 0.0
            lines = file_to_read.readline()  # 整行读取数据
            if not lines:
                break
                pass
            p_tmp = [float(i) for i in lines.split()]  # 将整行数据分割处理，如果分割符是空格，括号里就不用传入参数，如果是逗号， 则传入‘，'字符。
            index = district_dict[int(p_tmp[0])]
            p_tmp.pop(0)
            for i in range(len(p_tmp)):
                sum += p_tmp[i] ** 2
            sum_sqrt = sum ** 0.5
            flow_dict[index] = sum_sqrt
            pass
    pass

    # for values in flow_dict.values():
    #     sum_f = 0
    #     for i in range(len(value)):
    #         sum_f = sum_f + value[i] * value[i]
    #     sum_f = np.sqrt(sum_f)
    #     for i in range(len(value)):
    #         value[i] = value[i] / sum_f
    #
    # print(1)


if __name__ == '__main__':
    deal()

