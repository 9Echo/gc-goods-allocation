select t_b.*
from (
select
t_fj.f_docuno as f_docuno,
t_fj.org_unit_name as org_unit_name,
t_fj.f_crted_date as f_crted_date,
t_fj.productname as productname,
t_fj.itemid as itemid,
t_fj.order_zg00 as order_zg00,
t_fj.j_docuno as j_docuno,
t_fj.SJGBSL as SJGBSL,
t_y.waybill_no as waybill_no,
t_y.main_product_list_no as main_product_list_no,
t_y.create_date as create_date,
t_y.travel_no as travel_no
from (
select
taf.docuno as f_docuno,
taf.org_unit_name as org_unit_name,
taf.f_crted_date as f_crted_date,
taf.productname as productname,
taf.itemid as itemid,
taf.order_zg00 as order_zg00,
taj.docuno as j_docuno,
taj.SJGBSL as SJGBSL
from (
select
tf1.docuno as docuno,
tf1.org_unit_name as org_unit_name,
tf1.crted_date as f_crted_date,
tf2.productname as productname,
tf2.itemid as itemid,
tf2.order_zg00 as order_zg00
from (
SELECT
docuno, -- 发货通知单号
org_unit_name, -- 客户名称
crted_date, -- 创建时间
order_zg00, -- 总根数
fee_order, -- 运费
order_cal, -- 理重合计
bath_no, -- 批次号
data_address
FROM db_dw.ods_db_inter_t_keeperhd
where data_address = '0030'
and crted_date BETWEEN '2019-10-21 00:00:00' and '2019-10-25 00:00:00'
) as tf1
left join (
SELECT
docuno, -- 发货通知单号
itemid, -- 规格型号名称
productname, -- 品名
order_zg00 -- 总根数
FROM db_dw.ods_db_inter_t_keeperln
where productname = '螺旋焊管') as tf2
on tf1.docuno = tf2.docuno
where tf2.docuno is not null
) as taf
left join (
SELECT
docuno, -- 结算单号
HXDH,  -- 发货通知单号
org_unit_name, -- 客户名称
crted_date, -- 创建时间
f_amt, -- 运费
SJGBSL, -- 结算重量
data_address
FROM db_dw.ods_db_inter_t_orderhd
where data_address = '0030'
) as taj
on taf.docuno = taj.HXDH
where taj.docuno is not NULL
order by taf.docuno

) as t_fj
left join (

SELECT
waybill_no,
main_product_list_no,
create_date,
travel_no
FROM db_dw.ods_db_trans_t_waybill
where company_id = 'C000000888'

) as t_y
on t_y.main_product_list_no = t_fj.f_docuno
where travel_no is not null
group by t_y.main_product_list_no,t_y.travel_no
order by travel_no

















) as t_car
left join (

select
t_fj.f_docuno as f_docuno,
t_fj.org_unit_name as org_unit_name,
t_fj.f_crted_date as f_crted_date,
t_fj.productname as productname,
t_fj.itemid as itemid,
t_fj.order_zg00 as order_zg00,
t_fj.j_docuno as j_docuno,
t_fj.SJGBSL as SJGBSL,
t_y.waybill_no as waybill_no,
t_y.main_product_list_no as main_product_list_no,
t_y.create_date as create_date,
t_y.travel_no as travel_no
from (
select
taf.docuno as f_docuno,
taf.org_unit_name as org_unit_name,
taf.f_crted_date as f_crted_date,
taf.productname as productname,
taf.itemid as itemid,
taf.order_zg00 as order_zg00,
taj.docuno as j_docuno,
taj.SJGBSL as SJGBSL
from (
select
tf1.docuno as docuno,
tf1.org_unit_name as org_unit_name,
tf1.crted_date as f_crted_date,
tf2.productname as productname,
tf2.itemid as itemid,
tf2.order_zg00 as order_zg00
from (
SELECT
docuno, -- 发货通知单号
org_unit_name, -- 客户名称
crted_date, -- 创建时间
order_zg00, -- 总根数
fee_order, -- 运费
order_cal, -- 理重合计
bath_no, -- 批次号
data_address
FROM db_dw.ods_db_inter_t_keeperhd
where data_address = '0030'
and crted_date BETWEEN '2019-10-21 00:00:00' and '2019-10-25 00:00:00'
) as tf1
left join (
SELECT
docuno, -- 发货通知单号
itemid, -- 规格型号名称
productname, -- 品名
order_zg00 -- 总根数
FROM db_dw.ods_db_inter_t_keeperln
-- where productname = '螺旋焊管'
) as tf2
on tf1.docuno = tf2.docuno
where tf2.docuno is not null
) as taf
left join (
SELECT
docuno, -- 结算单号
HXDH,  -- 发货通知单号
org_unit_name, -- 客户名称
crted_date, -- 创建时间
f_amt, -- 运费
SJGBSL, -- 结算重量
data_address
FROM db_dw.ods_db_inter_t_orderhd
where data_address = '0030'
) as taj
on taf.docuno = taj.HXDH
where taj.docuno is not NULL
order by taf.docuno

) as t_fj
left join (

SELECT
waybill_no,
main_product_list_no,
create_date,
travel_no
FROM db_dw.ods_db_trans_t_waybill
where company_id = 'C000000888'

) as t_y
on t_y.main_product_list_no = t_fj.f_docuno
where travel_no is not null
group by t_y.main_product_list_no,t_y.travel_no
order by travel_no

) as t_b
on t_car.travel_no = t_b.travel_no
and abs(TIMESTAMPDIFF(MINUTE,t_b.create_date,t_car.create_date))<10
order by t_b.travel_no,t_b.create_date


