# 测试数据
{
	"data":
	[
	    {
	    	"source_number":"1",
	        "notice_num": "F2009070304",
	        "consumer": "山东东宏管业股份有限公司",
	        "oritem_num": "DF2009220006-003",
	        "deliware_house": "P5-P5冷轧成品库",
	        "commodity_name": "热轧卷板",
	        "big_commodity_name": "黑卷",
	        "detail_address": "山东省日照市岚山区岚山港",
	        "province":"山东省",
	        "city": "日照市",
	        "waint_fordel_number": "2",
	        "waint_fordel_weight": "66",
	        "can_send_number": "2",
	        "can_send_weight": "66",
	        "dlv_spot_name_end": "岚山区",
	        "pack_number": "0",
	        "need_lading_num": "0",
	        "need_lading_wt": "0",
	        "over_flow_wt": "0",
	        "latest_order_time": "20200920022706"
	    },
	    {
	    	"source_number":"2",
	        "notice_num": "F2009070000",
	        "consumer": "山东股份有限公司",
	        "oritem_num": "DF2009220006-000",
	        "deliware_house": "P5-P5冷轧成品库",
	        "commodity_name": "热轧卷板",
	        "big_commodity_name": "黑卷",
	        "detail_address": "山东省日照市岚山区岚山港",
	        "province":"山东省",
	        "city": "日照市",
	        "waint_fordel_number": "6",
	        "waint_fordel_weight": "36",
	        "can_send_number": "6",
	        "can_send_weight": "36",
	        "dlv_spot_name_end": "岚山区",
	        "pack_number": "0",
	        "need_lading_num": "0",
	        "need_lading_wt": "0",
	        "over_flow_wt": "0",
	        "latest_order_time": "20200920022706"
	    }
	]
}
# 结果数据
{
    "code": 100,
    "msg": "成功",
    "data": {
        "pick_list": [
            {
                "totalWeight": 66,
                "truckNum": 2,
                "province": "山东省",
                "city": "日照市",
                "endPoint": "岚山区",
                "bigCommodity": "新产品-卷板",
                "commodity": "热轧卷板",
                "remark": "新产品-卷板1件,一车33吨左右",
                "items": [
                    {
                        "totalWeight": 66,
                        "truckNum": 2,
                        "province": "山东省",
                        "city": "日照市",
                        "endPoint": "岚山区",
                        "bigCommodity": "新产品-卷板",
                        "commodity": "热轧卷板",
                        "remark": "新产品-卷板1件,一车33吨左右"
                    }
                ]
            },
            {
                "totalWeight": 30,
                "truckNum": 1,
                "province": "山东省",
                "city": "日照市",
                "endPoint": "岚山区",
                "bigCommodity": "新产品-卷板",
                "commodity": "热轧卷板",
                "remark": "新产品-卷板5件,一车30吨左右",
                "items": [
                    {
                        "totalWeight": 30,
                        "truckNum": 1,
                        "province": "山东省",
                        "city": "日照市",
                        "endPoint": "岚山区",
                        "bigCommodity": "新产品-卷板",
                        "commodity": "热轧卷板",
                        "remark": "新产品-卷板5件,一车30吨左右"
                    }
                ]
            }
        ],
        "tail_list": [
            {
                "source_number": "2",
                "noticeNum": "F2009070000",
                "oritemNum": "DF2009220006-000",
                "deliwareHouse": "P5-P5冷轧成品库",
                "sendNumber": 5,
                "sendWeight": 30,
                "packNumber": 5
            }
        ]
    }
}