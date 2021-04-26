from geopy import distance

from model_config import ModelConfig

lat = None
lon = None
driver_tuple = (lat, lon)
# 日钢位置：纬度、经度
rg_tuple = (ModelConfig.PICK_RG_LAT.get("日钢纬度"), ModelConfig.PICK_RG_LON.get("日钢经度"))
# 计算距离：千米
dist = distance.great_circle(driver_tuple, rg_tuple).km
print(dist)


