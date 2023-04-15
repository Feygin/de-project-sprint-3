-- загружаем новых покупателей d_customer из staging слоя в хранилище
-- если покупатель с таким идентификатором загружен в хранилище
-- но информация для него изменилась, то обновляем данные
-- так мы выполняем условие SCD 1 для хранилища
with s_customer as (
	select customer_id, first_name, last_name, max(city_id) as city_id
	from staging.user_order_log
	-- добавил фильтрацию за день, можно и без нее, но так должно быть быстрее, если в staging много данных
	where date_time::date = '{{ds}}' 
	group by customer_id, first_name, last_name
)
insert into mart.d_customer as m_c (customer_id, first_name, last_name, city_id)
select customer_id, first_name, last_name, city_id 
from s_customer as s_c
on conflict(customer_id) do update 
set first_name = excluded.first_name,
    last_name  = excluded.last_name,
    city_id    = excluded.city_id
where  m_c.first_name <> excluded.first_name
    or m_c.last_name  <> excluded.last_name
    or m_c.city_id    <> excluded.city_id;