select *
	from default.dump_orders_prod
		where pos_product_num = 0;

select *
	from default.dict_gln
		where admin1_code='71' --Свердловская область
			or admin1_code is null;
		
select *
	from dict_account;

select DISTINCT okvedCode
	from default.dict_inn_gln_okved okvd
		where lower(okvedName) like '%хлеб%';
		
--Список позиций в заказах
select distinct pos_description
	from default.dump_orders_prod
order by 1;
	
--Список позиций по производителям хлеба
select pos_description
	from (
			select okvedName, okvedCode, ord_number, ord_date, ord_buyer_gln, ord_supplier_gln, pos_product_num, pos_orderedquantity_x1k/1000 pos_orderedquantity_x1k, pos_description
			,varname, varstreet, varcityregexp, name_rus, name_utf8, admin1_code
				from (
						select okvedName, okvedCode, ord.ord_number, ord_date, ord_buyer_gln, ord_supplier_gln, pos_product_num, pos_orderedquantity_x1k, pos_description
							from default.dump_orders_prod ord
								any left join (select gln as ord_supplier_gln, okvedName, okvedCode
												from default.dict_inn_gln_okved) okvd		 
									using ord_supplier_gln) ord1
					any left join (select gln as ord_supplier_gln, varname, varstreet, varcityregexp, name_rus, name_utf8, admin1_code
									from default.dict_gln) gln1
						using ord_supplier_gln
				where admin1_code='71'
					and (lower(okvedName) like '%хлеб%'
						or length(okvedName)=0)
	)
group by pos_description
order by pos_description;

--Данные по производителям по ОКВЭД_201809190819.csv

select ord_supplier_gln, supplier_name, accountname, suppliercity, okvedName, okvedCode, pos_product_num, pos_name, group_name, ord_date, ord_number, pos_orderedquantity, weight_kg, weight_total
	from (
			select ord_supplier_gln, varname supplier_name, okvedName, okvedCode, pos_product_num, pos_name, group_name, intaccountid, varcityregexp as suppliercity, ord_date, ord_number, pos_orderedquantity, weight_kg, weight_total
				from ( 
						select ord_supplier_gln, okvedName, okvedCode, ord.pos_product_num, ord.ord_date, ord_number, pos_orderedquantity, weight_kg, pos_orderedquantity*weight_kg as weight_total, pos_name, group_name
							from (
									select ord.ord_supplier_gln, okvedName, okvedCode, pos_product_num, pos_orderedquantity_x1k/1000 pos_orderedquantity, ord.ord_date, ord_number
										from default.dump_orders_prod ord
											any left join (select gln as ord_supplier_gln, okvedName, okvedCode from default.dict_inn_gln_okved okvd) okvd		 
												using ord_supplier_gln
											where ord_date between '2018-07-01' and '2018-07-31') ord
								any inner join (select gtin as pos_product_num, pos_name, group_name, weight_gramm/1000 weight_kg from dict_gtin_groups_bread) gtin
									using pos_product_num)
						any inner join (select gln as ord_supplier_gln, varname, varstreet, varcityregexp, name_rus, name_utf8, admin1_code, intaccountid
											from default.dict_gln) gln1
							using ord_supplier_gln
				where admin1_code='71')
		any left join (select intAccountID as intaccountid, varCompanyName as accountname from dict_account)
			using intaccountid;