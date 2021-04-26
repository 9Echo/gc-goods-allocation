from datetime import datetime


class GenerateId:
    """
    全局车次号
    Args:

    Returns:

    Raise:

    """
    train_id = 0
    stock_id = 0
    overall_time = None

    @staticmethod
    def get_id():
        if not GenerateId.overall_time:
            GenerateId.overall_time = datetime.now().strftime("%Y%m%d%H%M")
        GenerateId.train_id += 1
        return GenerateId.overall_time + str(GenerateId.train_id).zfill(4)

    @staticmethod
    def get_surplus_id():
        if not GenerateId.overall_time:
            GenerateId.overall_time = datetime.now().strftime("%Y%m%d%H%M")
        return GenerateId.overall_time + '0000'

    @staticmethod
    def set_id():
        GenerateId.train_id = 0
        GenerateId.stock_id = 0
        GenerateId.overall_time = None

    @staticmethod
    def get_stock_id():
        GenerateId.stock_id += 1
        return GenerateId.stock_id


class HashKey:
    @staticmethod
    def get_key(key_list):
        return ",".join(key_list)


if __name__ == "__main__":
    for i in range(10):
        print(GenerateId.get_id())
