import pandas as pd
import datetime
from app.main.steel_factory.dao.no_interest_count_dao import no_interest_count_dao


def blacklist_timer():
    # 读取message_log表和driver_action表中前一天的记录 -》 将两张表按driver_id,pickup_no合并
    message_log_df = no_interest_count_dao.log_info_select()  # 前一天短信推送信息
    driver_action_df = no_interest_count_dao.driver_records_select()  # 前一天司机的摘单浏览行为
    if message_log_df.empty or driver_action_df.empty:  # 如果有一个为空，返回
        return
    action_message_df = driver_action_df.merge(message_log_df, how='left', indicator=True,
                                               left_on=['driver_id', 'pickup_no'],
                                               right_on=['driver_id', 'pickup_no'])
    # 选出发了短信的 -》 升序排序
    have_message_df = action_message_df.loc[action_message_df['_merge'] == 'both']
    have_message_df = have_message_df.sort_values(by=['create_date'], ascending=[True])
    # 更新无兴趣计数表
    uninterest_count_df = no_interest_count_dao.no_interest_count_select()
    if uninterest_count_df.empty:
        uninterest_count_df = pd.DataFrame(columns=['driver_id', 'district', 'product_name', 'count', 'update_time'])
    for idx, row in uninterest_count_df.iterrows():
        update_time = row['update_time']
        current_time = datetime.datetime.now()
        days_diff = (current_time - update_time).days
        if days_diff >= 15:
            driver_id = row['driver_id']
            district = row['district']
            product_name = row['product_name']
            uninterest_count_df.loc[(uninterest_count_df['driver_id'] == driver_id)
                                    & (uninterest_count_df['district'] == district)
                                    & (uninterest_count_df['product_name'] == product_name), 'count'] = 0

    for index, row in have_message_df.iterrows():
        driver_id = row['driver_id']
        district = row['district_name_x']
        prod_name = row['prod_name']
        page = row['page']
        # update_time = pd.Timestamp(row['create_date'],tz=None)   # 更新时间设置为短信推送时间
        # update_time = update_time.to_pydatetime()
        update_time = row['create_date']
        # update_time = datetime.datetime.strptime(str(row['create_date']),'%Y-%m-%d %H:%M:%S')
        interested = True
        if pd.isnull(page):  # 不感兴趣
            interested = False

        if uninterest_count_df[(uninterest_count_df['driver_id'] == driver_id)
                               & (uninterest_count_df['district'] == district)
                               & (uninterest_count_df['product_name'] == prod_name)].empty:  # 如果原表没有匹配项
            new_dict = {'driver_id': driver_id, 'district': district, 'product_name': prod_name, 'count': -1,
                        'update_time': update_time}
            if interested:
                new_dict['count'] = 0
            else:
                new_dict['count'] = 1
            # 添加新的项
            uninterest_count_df = uninterest_count_df.append([new_dict], ignore_index=False)
        else:
            if interested:
                uninterest_count_df.loc[(uninterest_count_df['driver_id'] == driver_id)
                                        & (uninterest_count_df['district'] == district)
                                        & (uninterest_count_df['product_name'] == prod_name), 'count'] = 0
                uninterest_count_df.loc[(uninterest_count_df['driver_id'] == driver_id)
                                        & (uninterest_count_df['district'] == district)
                                        & (uninterest_count_df[
                                               'product_name'] == prod_name), 'update_time'] = update_time
            else:
                uninterest_count_df.loc[(uninterest_count_df['driver_id'] == driver_id)
                                        & (uninterest_count_df['district'] == district)
                                        & (uninterest_count_df['product_name'] == prod_name), 'count'] += 1
                uninterest_count_df.loc[(uninterest_count_df['driver_id'] == driver_id)
                                        & (uninterest_count_df['district'] == district)
                                        & (uninterest_count_df[
                                               'product_name'] == prod_name), 'update_time'] = update_time

    # 写入更新表
    # values = uninterest_count_df.to_dict('records')
    new_values = []
    values = [list(x) for x in uninterest_count_df.values]
    for new_list in values:
        new_list[-1] = datetime.datetime.strptime(str(new_list[-1]), '%Y-%m-%d %H:%M:%S')
        new_tuple = tuple(new_list)
        new_values.append(new_tuple)
    no_interest_count_dao.no_interest_count_update(new_values)

# if __name__ == "__main__":
#     # blacklist_init()
#     blacklist_timer()
# 如今的问题是什么，最终的目标是什么
# 机器学习可以分为几类问题
