from app.main.pipe_factory.rule.lp_problem_solution import call_pulp_solve, create_variable_list


# def weight_filter(delivery_item_weight_list,new_max_weight):
def weight_filter(delivery_item_weight_list):
    weight_list, volume_list, value_list = create_variable_list(delivery_item_weight_list)
    sheets = call_pulp_solve(weight_list, volume_list, value_list, delivery_item_weight_list)
    return sheets
