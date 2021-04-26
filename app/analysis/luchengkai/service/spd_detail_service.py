import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def spd_detail(df):
    waybill_no_list = df['waybill_no'].unique()
    for i in range(len(waybill_no_list)):
        df_plt = df[df['waybill_no'] == waybill_no_list[i]]
        # print(df_plt['create_date'])
        # 将字符串的日期，转换成日期对象
        # xs = [datetime.datetime.strptime(d, '%Y%m%d').date() for d in df_plt['create_date']]
        plt.subplot(len(waybill_no_list), 1, i + 1)
        fig = plt.figure(figsize=(8, 4))
        ax = fig.add_subplot(111)
        ax.plot(df_plt['create_date'], df_plt['spd'], color='r')

        # labels = [0, 20, 40, 60, 80, 100]

        plt.title(waybill_no_list[i])
        plt.ylabel('speed')
        # 日期对象作为参数设置到横坐标,并且使用list_date中的字符串日志作为对象的标签（别名）
        plt.xticks(df_plt['create_date'], rotation=45, fontsize=10)
        # plt.yticks(df_plt['spd'], labels, fontsize=10)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d %H'))  # 設置x軸主刻度顯示格式（日期）
        plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=1))  # 設置x軸主刻度間距
        # plt.show()
        plt.savefig("C:/Users/13908/Desktop/spd_detail/"+waybill_no_list[i]+".png")
        # for x, y in zip(xs, df_plt['spd']):
        #     plt.text(x, y + 0.3, str(y), ha='center', va='bottom', fontsize=10)

