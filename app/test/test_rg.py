# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/6/22 17:31
RG_COMMODITY_GROUP = {'老区-型钢': ['老区-型钢'],
                      '老区-线材': ['老区-线材'],
                      '老区-螺纹': ['老区-螺纹'],
                      '新产品-白卷': ['新产品-开平板', '新产品-窄带', '新产品-冷板', '新产品-白卷', '老区-卷板', '新产品-卷板'],
                      '老区-卷板': ['新产品-白卷', '老区-卷板', '新产品-卷板', '老区-线材'],
                      '新产品-卷板': ['新产品-开平板', '新产品-窄带', '新产品-冷板', '新产品-白卷', '老区-卷板', '新产品-卷板'],
                      '新产品-开平板': ['新产品-开平板', '新产品-窄带', '新产品-冷板', '新产品-白卷', '新产品-卷板', '新产品-开平板'],
                      '新产品-窄带': ['新产品-开平板', '新产品-窄带', '新产品-冷板', '新产品-白卷', '新产品-卷板'],
                      '新产品-冷板': ['新产品-开平板', '新产品-窄带', '新产品-冷板', '新产品-白卷', '新产品-卷板'],
                      '老区-开平板': ['新产品-开平板', '老区-开平板']
                      }
RG_COMMODITY_GROUP_FOR_SQL = {'老区-型钢': ['型钢'],
                              '老区-线材': ['线材'],
                              '老区-螺纹': ['螺纹'],
                              '老区-开平板': ['开平板'],
                              '老区-卷板': ['黑卷', '白卷', '线材'],
                              '新产品-白卷': ['开平板', '窄带', '冷板', '白卷', '黑卷'],
                              '新产品-卷板': ['白卷', '黑卷', '线材', '开平板', '窄带', '冷板'],
                              '新产品-开平板': ['开平板', '窄带', '冷板', '白卷', '黑卷'],
                              '新产品-窄带': ['开平板', '窄带', '冷板', '白卷', '黑卷'],
                              '新产品-冷板': ['开平板', '窄带', '冷板', '白卷', '黑卷']
                              }
RG_COMMODITY_LYG = ["老区-卷板", "新产品-卷板", "新产品-白卷"]
RG_VARIETY_VEHICLE = {
    "老区-型钢": ["垫木", "垫皮", "钢丝绳"],
    "老区-线材": ["垫木"],
    "老区-螺纹": ["垫木"],
    "老区-卷板": ["鞍座", "草垫子", "垫皮", "垫木"],
    "新产品-白卷": ["鞍座", "草垫子", "垫皮", "垫木"],
    "新产品-卷板": ["鞍座", "草垫子", "垫皮", "垫木"],
    "新产品-开平板": ["垫木"],
    "新产品-窄带": ["钢丝绳", "垫皮", "垫木"],
    "新产品-冷板": ["钢丝绳"],
    "老区-开平板": ["垫木"]
}
