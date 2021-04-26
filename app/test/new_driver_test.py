from app.main.steel_factory.rule.pick_propelling_label_rule import pick_cold_start
from app.main.steel_factory.entity.pick_propelling import PickPropelling
def f():
    new_prop = PickPropelling()
    new_prop.city_name = '枣庄市'
    pick_cold_start([new_prop])

if __name__=="__main__":
    f()