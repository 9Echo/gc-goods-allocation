def f():
    from typing import List

    s:List[str] = []
    s.append('s')
    print(s)

def f2():
    list1 = [1,2,3,4]
    list2 = list1
    list2[2] = 8
    print(list1)
    list3 = list1[:]
    list3[2] = 5
    print(list1)

def f3():
    import copy
    names = ['zhao', 'qian', ['sun', 'li'], 'zhou']
    names2 = copy.deepcopy(names)
    print(names, names2)
    names[2][0] = 'wu'
    print(names, names2)

def f4():
    newlist = [1,2,3,4]

def f5():
    class Student:  # 定义类
        def __init__(self, name, identity, age):
            self._name = name
            self._identity = identity
            self.age = age

        def __getitem__(self, item):
            if isinstance(item, str):
                return getattr(self, "_item")


if __name__=="__main__":
    f3()