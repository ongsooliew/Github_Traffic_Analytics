select 
committername,
committeremail,
date(committerdate) as dt
from T2_commit_traffic
group by 1,2,3
order by 1,2,3 asc;