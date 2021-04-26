from model_config import ModelConfig


def consumer():
    r = ''
    ModelConfig.MAX_WEIGHT = 1
    while True:
        if ModelConfig.MAX_WEIGHT > 0:
            n = yield r
            ModelConfig.MAX_WEIGHT -= 1
            print('ss')
            if not n:
                return
            print('[CONSUMER] Consuming %s...' % n)
            r = '200 OK'


def produce(c):
    c.send(None)
    n = 0

    while n < 5:
        n = n + 1
        if ModelConfig.MAX_WEIGHT > 0:
            ModelConfig.MAX_WEIGHT -= 1
            print(ModelConfig.MAX_WEIGHT)
            print('[PRODUCER] Producing %s...' % n)
            r = c.send(n)
            print('[PRODUCER] Consumer return: %s' % r)
    c.close()


if __name__ == "__main__":
    c = consumer()
    produce(c)
