invalidate metadata;

select url, count(*) as total_accesos
from logs.tokenized_access_logs
where url like '%\/product\/%'
group by url 
order by count(*) desc;