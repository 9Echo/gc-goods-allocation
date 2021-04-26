from app.util.uuid_util import UUIDUtil


def replenish_property(sheets, order, batch_no, types):
    """

    :param types:
    :param batch_no:
    :param order:
    :param sheets:
    :return:
    """
    for sheet in sheets:
        sheet.batch_no = batch_no
        sheet.customer_id = order.customer_id
        sheet.salesorg_id = order.salesorg_id
        sheet.request_id = order.request_id
        sheet.type = types
        sheet.salesman_id = order.salesman_id
        sheet.weight = 0
        sheet.total_pcs = 0
        for di in sheet.items:
            # di.delivery_item_no = UUIDUtil.create_id("di")
            sheet.weight += di.weight
            sheet.total_pcs += di.total_pcs
