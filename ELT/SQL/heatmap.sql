with t1 as (
select 
	to_char(date(committerdate), 'Day') as weekday,
	committerdate::time as t
from t2_commit_traffic
)
select 
	trim(both from weekday) as weekday,
	case 
	when t between '00:00:00' and '03:00:00' then '00-03'
	when t between '03:00:01' and '06:00:00' then '03-06'
	when t between '06:00:01' and '09:00:00' then '06-09'
	when t between '09:00:01' and '12:00:00' then '09-12'
	when t between '12:00:01' and '15:00:00' then '12-15'
	when t between '15:00:01' and '18:00:00' then '15-18'
	when t between '18:00:01' and '21:00:00' then '18-21'
	when t between '21:00:01' and '23:59:59' then '21-00'
	end as category
	from t1;