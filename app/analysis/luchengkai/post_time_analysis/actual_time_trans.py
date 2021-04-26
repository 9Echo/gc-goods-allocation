import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MultipleLocator

from app.analysis.luchengkai.rule import swap_rule, free_time_rule
from app.analysis.luchengkai.service import free_time_service, spd_detail_service

if __name__ == '__main__':
    # data = openpyxl.load_workbook('C:/Users/13908/Desktop/car_track.xlsx')
    file = "C:/Users/13908/Desktop/car_track_11.xlsx"
    df = pd.read_excel(file, usecols=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    '''free_time_statistic'''
    # free_time_quantum
    # flag = free_time_service.free_time_statistic(df, 'free_time_quantum')
    # free_time_detail
    flag = free_time_service.free_time_statistic(df, 'free_time_detail')

    '''spd_detail'''
    # flag = spd_detail_service.spd_detail(df)





