--13 335 544
select count(DISTINCT intDocID) cntOrdIntDocID
	from dump_orders_prod;

--Проверяем качество данных
select intDocID, recadv_ord_date, * 
	from dump_recadv_prod
		where recadv_date >= '2018-07-01'
			and (recadv_date = '1000-01-01'
			or recadv_ord_number = '-'
			or recadv_buyer_gln = 0
			or recadv_supplier_gln = 0
			or recadv_ord_date = '1000-01-01');

--Куча документов без номера заказа			
select intDocID, recadv_ord_date, * 
	from dump_recadv_prod
		where recadv_ord_number = '-'
			and recadv_date >= '2018-07-01';

--Определяем время между заказом и recadv сопоставляя их по поставщику, покупателю и дате заказа
--Получается фигня какая то
--2 880 044
--2 495 418
--362 906
--17 653	
select count(1) cnt_all
,sum(case when ediDuration between 0 and 7 then 1 else 0 end) durationLessOneWeek
,sum(case when ediDuration between 8 and 30 then 1 else 0 end) durationFromOneWeekToOneMonth
,sum(case when ediDuration > 30 then 1 else 0 end) durationFromOneMonth
	from (
			select ord.*, rec.recadv_date, rec.recadv_date-ord.ord_date ediDuration
				from (select distinct ord_number, ord_date, ord_buyer_gln, ord_supplier_gln
						from dump_orders_prod ord
							where ord_date BETWEEN '2018-07-01' and '2018-08-01') ord	
					any inner join (select distinct recadv_ord_date ord_date, recadv_ord_number, recadv_date, recadv_buyer_gln ord_buyer_gln, recadv_supplier_gln ord_supplier_gln
										from dump_recadv_prod) rec
						using ord_supplier_gln, ord_buyer_gln, ord_date);

--Определяем время между заказом и recadv сопоставляя их по поставщику, покупателю и дате заказа и GTIN
--В один запрос не хватает памяти. Поэтому делаем через временную таблицу
select count(1) cnt_all
,sum(case when ediDuration between 0 and 7 then 1 else 0 end) durationLessOneWeek
,sum(case when ediDuration between 8 and 30 then 1 else 0 end) durationFromOneWeekToOneMonth
,sum(case when ediDuration > 30 then 1 else 0 end) durationFromOneMonth
	from (
select recadv_date-ord_date ediDuration
	from (
			select DISTINCT ord_supplier_gln, ord_buyer_gln, ord_date, pos_product_num, rec.recadv_date
				from (select distinct ord_number, ord_date, ord_buyer_gln, ord_supplier_gln, pos_product_num
						from dump_orders_prod ord
							where ord_date BETWEEN '2018-07-01' and '2018-08-01') ord	
					any inner join (select distinct recadv_ord_date ord_date, recadv_ord_number, recadv_date, recadv_buyer_gln ord_buyer_gln, recadv_supplier_gln ord_supplier_gln, recadv_pos_product_num pos_product_num
										from dump_recadv_prod) 
										
										rec
						using ord_supplier_gln, ord_buyer_gln, ord_date, pos_product_num) ord);

--Создаем временную таблицу
--drop TABLE del_me_tmp_edi_duration_0;
--select * from del_me_tmp_edi_duration_0;
--select count() from del_me_tmp_edi_duration_0;
create TABLE del_me_tmp_edi_duration_0
(ord_supplier_gln Int64
,ord_buyer_gln Int64
,ord_date Date
,pos_product_num Int64
,recadv_date Date
,ord_intDocID Int64
,rec_intDocID Int64)
engine = MergeTree partition by recadv_date order by ord_supplier_gln;

--Загружаем первую часть данных
insert into del_me_tmp_edi_duration_0
select DISTINCT ord_supplier_gln, ord_buyer_gln, ord_date, pos_product_num, rec.recadv_date, ord_intDocID, rec_intDocID
				from (select distinct ord_number, ord_date, ord_buyer_gln, ord_supplier_gln, pos_product_num, intDocID ord_intDocID
						from dump_orders_prod ord
							where ord_date BETWEEN '2018-07-01' and '2018-07-10') ord	
					any inner join (select distinct recadv_ord_date ord_date, recadv_ord_number, recadv_date, recadv_buyer_gln ord_buyer_gln, recadv_supplier_gln ord_supplier_gln, recadv_pos_product_num pos_product_num, intDocID rec_intDocID 
										from dump_recadv_prod) rec
						using ord_supplier_gln, ord_buyer_gln, ord_date, pos_product_num;

--Загружаем вторую часть данных
insert into del_me_tmp_edi_duration_0
select DISTINCT ord_supplier_gln, ord_buyer_gln, ord_date, pos_product_num, rec.recadv_date, ord_intDocID, rec_intDocID
				from (select distinct ord_number, ord_date, ord_buyer_gln, ord_supplier_gln, pos_product_num, intDocID ord_intDocID
						from dump_orders_prod ord
							where ord_date BETWEEN '2018-07-11' and '2018-07-15') ord	
					any inner join (select distinct recadv_ord_date ord_date, recadv_ord_number, recadv_date, recadv_buyer_gln ord_buyer_gln, recadv_supplier_gln ord_supplier_gln, recadv_pos_product_num pos_product_num, intDocID rec_intDocID 
										from dump_recadv_prod) rec
						using ord_supplier_gln, ord_buyer_gln, ord_date, pos_product_num;

--Загружаем третью часть данных
insert into del_me_tmp_edi_duration_0
select DISTINCT ord_supplier_gln, ord_buyer_gln, ord_date, pos_product_num, rec.recadv_date, ord_intDocID, rec_intDocID
				from (select distinct ord_number, ord_date, ord_buyer_gln, ord_supplier_gln, pos_product_num, intDocID ord_intDocID
						from dump_orders_prod ord
							where ord_date BETWEEN '2018-07-15' and '2018-07-21') ord	
					any inner join (select distinct recadv_ord_date ord_date, recadv_ord_number, recadv_date, recadv_buyer_gln ord_buyer_gln, recadv_supplier_gln ord_supplier_gln, recadv_pos_product_num pos_product_num, intDocID rec_intDocID 
										from dump_recadv_prod) rec
						using ord_supplier_gln, ord_buyer_gln, ord_date, pos_product_num;

--Загружаем четвертую часть данных
insert into del_me_tmp_edi_duration_0
select DISTINCT ord_supplier_gln, ord_buyer_gln, ord_date, pos_product_num, rec.recadv_date, ord_intDocID, rec_intDocID
				from (select distinct ord_number, ord_date, ord_buyer_gln, ord_supplier_gln, pos_product_num, intDocID ord_intDocID
						from dump_orders_prod ord
							where ord_date BETWEEN '2018-07-22' and '2018-08-01') ord	
					any inner join (select distinct recadv_ord_date ord_date, recadv_ord_number, recadv_date, recadv_buyer_gln ord_buyer_gln, recadv_supplier_gln ord_supplier_gln, recadv_pos_product_num pos_product_num, intDocID rec_intDocID 
										from dump_recadv_prod) rec
						using ord_supplier_gln, ord_buyer_gln, ord_date, pos_product_num;
						
--Считаем
--3315476
--1484
--3162035
--274951
--19619
select count(DISTINCT ord_intDocID) cnt_all
--,count(distinct case when recadv_date-ord_date < 0 then ord_intDocID else null end) durationLessZero
,count(distinct case when recadv_date-ord_date between 0 and 7 then ord_intDocID else null end) durationLessOneWeek
,count(distinct case when recadv_date-ord_date between 8 and 30 then ord_intDocID else null end) durationFromOneWeekToOneMonth
,count(distinct case when recadv_date-ord_date > 30 then ord_intDocID else null end) durationFromOneMonth
	from del_me_tmp_edi_duration_0;
	
--Проверяем
ord 2238498971
rec 2308661165
ord 2233454085
rec 2309529189
ord 2237986355
rec 2316654731
select *
	from del_me_tmp_edi_duration_0
		where recadv_date-ord_date > 30;

--Проверяем отрицательные
ord 2260953987
rec 2271663771
select *
	from del_me_tmp_edi_duration_0
		where recadv_date-ord_date < 0;
