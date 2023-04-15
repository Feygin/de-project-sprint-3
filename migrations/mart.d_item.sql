-- загружаем новые товары d_item аналогично d_customer
with s_item as (
	select distinct item_id, item_name 
	from staging.user_order_log
	where date_time::date = '{{ds}}'
)
insert into mart.d_item as m_i (item_id, item_name)
select item_id, item_name 
from s_item as s_i
on conflict (item_id) do update
set item_name = excluded.item_name  
where m_i.item_name = excluded.item_name;