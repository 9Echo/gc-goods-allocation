SELECT
	distinct t1.*,
	t2.*,
	t3.* ,
	t4.*
FROM
	(
SELECT
	contract_effective_date,
	contract_effective_time,
	substring( sales_contract_no, 2 ) AS sales_contract_no,
	contract_statusx
FROM
	`ods_db_inter_t_sales_contract_rg`
WHERE
	trans_type = '汽运'
	AND create_date BETWEEN '2020-03-31 00:00:00'
	AND '2020-03-31 23:59:59'
	AND destination_address LIKE '山东%'
	AND contract_status != '结案'
	) t1
	LEFT JOIN
	( SELECT
	substring_index( oritem_num, '-', 1 ) AS oritem_num
	FROM
	ods_db_inter_bclp_bill_of_loading_no ) t2
	ON
	t1.sales_contract_no = t2.oritem_num
	LEFT JOIN
	( SELECT
	substring_index( ORITEMNUM, '-', 1 ) AS ORITEMNUM, creattime
	FROM
	ods_db_inter_bclp_can_be_send_amount_log ) t3
	ON
	t1.sales_contract_no = t3.ORITEMNUM
	LEFT JOIN
	( SELECT
	substring_index ( oritem_num, '-', 1 ) as oritem_num
	FROM
	 ods_db_inter_bclp_bill_of_loading_no_detail_n ) t4
	 ON
	 t1.sales_contract_no = t4.oritem_num
	where t4.oritem_num is null