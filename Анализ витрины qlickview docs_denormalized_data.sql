select varCompanyName,
intTypeID,
count(1) cnt
	from default.docs_denormalized_data
		where varIndex between cast('2018-09-01 00:00:01' as DateTime) and cast('2018-09-30 23:59:59' as DateTime)
			and intTypeID in (2,3,4,5)
			and varCompanyName = 'Ваш Хлеб'
group by varCompanyName, intTypeID
order by varCompanyName, intTypeID