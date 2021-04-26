import csv

from app.analysis.rules import dispatch_filter
from app.main.dao.delivery_sheet_dao import delivery_sheet_dao
from app.main.entity.delivery_item import DeliveryItem
from app.util.uuid_util import UUIDUtil

if __name__ == '__main__':
    file = open('output.csv', 'r', encoding='utf-8')
    reader = csv.reader(file)
    total_items = []
    for row in reader:
        item = DeliveryItem()
        item.delivery_item_no = UUIDUtil.create_id("di")
        item.id = row[2]
        item.spec = row[6]
        item.product_type = row[7]
        item.quantity = row[11]
        if item.quantity == '':
            item.quantity = 0
        item.free_pcs = row[12]
        if item.free_pcs == '':
            item.free_pcs = 0
        item.customer_id = row[5]
        item.salesman_id = row[14]
        item.weight = int(row[3])
        item.create_time = row[9]
        total_items.append(item)
    item_dict = {}
    for item in total_items:
        item_dict.setdefault(item.id, []).append(item)
    for items in item_dict.values():
        while items:
            sheets = dispatch_filter.filter(items)
            if sheets:
                for sheet in sheets:
                    sheet.delivery_no = UUIDUtil.create_id("ds")
                    sheet.customer_id = sheet.items[0].customer_id
                    sheet.salesman_id = sheet.items[0].salesman_id
                    sheet.create_time = sheet.items[0].create_time
                    sheet.weight = 0
                    for di in sheet.items:
                        di.delivery_item_no = UUIDUtil.create_id("di")
                        di.delivery_no = sheet.delivery_no
                        sheet.weight += di.weight
                delivery_sheet_dao.batch_insert(sheets)