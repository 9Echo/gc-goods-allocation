import multiprocessing
from multiprocessing import Manager


# import numpy as np
#
# A = np.asarray(((1, 2),
#                 (3, 4),
#                 (5, 6)))
# # print((A%1).sum())
# print(A.sum(axis=0)[0])
# np.sum(A)
# np.sum(A, axis=0)
# np.array([9, 12])
# np.sum(A, axis=1)
# np.array([3, 7, 11])


def worker(procnum, return_dict):
    '''worker function'''
    print(str(procnum) + ' represent!')
    return_dict[procnum] = procnum


if __name__ == '__main__':
    manager = Manager()
    return_dict = manager.dict()
    p = multiprocessing.Pool(5)
    for i in range(5):
        p.apply_async(worker, (i, return_dict))
    p.close()  # 关闭进程池，关闭后po不再接收新的请求
    p.join()  # 等待po中所有子进程执行完成，必须放在close语句之后

    # for proc in jobs:
    #     proc.join()
    # 最后的结果是多个进程返回值的集合
    print(return_dict.values())
