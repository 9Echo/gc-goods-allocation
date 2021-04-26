# from geopy import distance
#
# lon = 71614302 / 600000
# lat = 21120145 / 600000
# dist = distance.great_circle((lat, lon), ("35.1582116300", "119.3315992000")).km
# dist2 = distance.great_circle(("35.1575390625", "119.33629231770833"), ("35.1582116300", "119.3315992000")).km
# print("车距离日钢" + str(dist) + "公里")
# print("司机距离日钢" + str(dist2) + "公里")


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
