-- загружаем новые города d_city аналогично d_customer
with s_city as (
	select distinct city_id, city_name 
	from staging.user_order_log as s_c
	where date_time::date = '{{ds}}'
)
insert into mart.d_city as m_c (city_id, city_name)
select city_id, city_name 
from s_city as s_c
on conflict(city_id) do update
set city_name = excluded.city_name
where m_c.city_name <> excluded.city_name;