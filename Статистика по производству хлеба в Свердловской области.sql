select *
	from default.dict_gln
		where admin1_code='71'; --Свердловская область

select okvedName, varname as supplier_name, varstreet, varcityregexp, name_rus, name_utf8
	from (
select okvedName, ord_number, ord_date, ord_buyer_gln, ord_supplier_gln, pos_product_num, pos_orderedquantity_x1k/1000 pos_orderedquantity_x1k, pos_description
,varname, varstreet, varcityregexp, name_rus, name_utf8, admin1_code
	from (
			select okvedName, ord.ord_number, ord_date, ord_buyer_gln, ord_supplier_gln, pos_product_num, pos_orderedquantity_x1k, pos_description
				from default.dump_orders_prod ord
					any left join (select gln as ord_supplier_gln, okvedName
									from default.dict_inn_gln_okved okvd) okvd		 
						using ord_supplier_gln) ord1
		any left join (select gln as ord_supplier_gln, varname, varstreet, varcityregexp, name_rus, name_utf8, admin1_code
						from default.dict_gln gln) gln1
			using ord_supplier_gln
	where admin1_code='71'
	)
group by okvedName, varname, varstreet, varcityregexp, name_rus, name_utf8
order by okvedName, varname, varstreet, varcityregexp, name_rus, name_utf8
;