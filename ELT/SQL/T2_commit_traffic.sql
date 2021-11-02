

create materialized view T2_commit_traffic as(
with t1 as(
select 
	sha, 
	committerdate, 
	case when committername='GitHub' then authorname else committername end as committername, 
	case when committeremail='noreply@github.com' then authoremail else committeremail end as committeremail, 
	elt_timestamp
from raw_commit_traffic 
where committerdate >= '{}'
),
t2 as (
select 
	*,
	row_number() over(PARTITION by sha order by elt_timestamp desc) rn
from t1
)
select 
    sha,
    committerdate,
    committername,
    committeremail
    from t2 where rn=1);