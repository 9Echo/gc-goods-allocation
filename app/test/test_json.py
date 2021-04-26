import json


class Customer:
    def __init__(self, name, grade, age, home, office):
        self.name = name
        self.grade = grade
        self.age = age
        self.address = Address(home, office)

    def __repr__(self):
        return repr((self.name, self.grade, self.age, self.address.home,
                    self.address.office))


class Address:
    def __init__(self, home, office):
        self.home = home
        self.office = office

    def __repr__(self):
        return repr((self.name, self.grade, self.age))


if __name__ == "__main__":
    customers = [
        Customer('john', 'A', 15, '111', 'aaa办公室'),
        Customer('jane', 'B', 12, '222', 'bbb办公室'),
        Customer('dave', 'B', 10, '333', 'ccc办公室')]

    json_str = json.dumps(
        customers[0].address.__dict__, sort_keys=True,
        ensure_ascii=False, indent=4)
    print(json_str)
