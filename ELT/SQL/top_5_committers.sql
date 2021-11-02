
with t1 as(
select 
	*,
	count(sha) over () as total_commits
from T2_commit_traffic
),
t2 as (
select 
	committername,
	committeremail,
	count(sha) as personal_commits,
	avg(total_commits) as total_commits
from t1
group by committername, committeremail
)
select * from t2
order by personal_commits desc 
limit 5;