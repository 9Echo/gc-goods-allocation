import os
import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox
import pandas as pd
from pandas.core.frame import DataFrame
import time

root = tk.Tk()
root.withdraw()
Folderpath = filedialog.askdirectory()
print(Folderpath)
files = os.listdir(Folderpath)
index = 0
s = []
df = pd.read_excel(Folderpath + '\\test.xls')
my_dict = df.to_dict(orient='records')
# df_dict = DataFrame(my_dict)


if len(files) != 0:
    for file in files:
        try:
            old_name = os.path.join(Folderpath, file)
            for i in my_dict:
                if file == i.get('原文件名'):
                    new_file = i.get('新文件名')
                    new_name = os.path.join(Folderpath, new_file)
                    os.rename(old_name, new_name)
                    i['执行结果'] = "匹配已修改"
                else:
                    i['执行结果'] = "未匹配"
        except Exception as e:
            pass
    print(my_dict)
    print(df)
    df_dict = DataFrame(my_dict)
    df_dict.to_excel(Folderpath + '\\test.xls', index=False)
    print(df_dict)
    messagebox.showinfo("提示", "执行结束")
else:
    messagebox.showinfo("提示", "选择文件夹为空")
    time.sleep(3)
