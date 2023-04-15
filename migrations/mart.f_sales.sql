-- загружаем факты продаж f_sales
-- не забываем удалить данные за день, чтобы выполнялось условие идемпотентности
delete from mart.f_sales m_s
where m_s.date_id in (
    select dc.date_id 
    from staging.user_order_log uol 
    join mart.d_calendar dc 
        on uol.date_time::date = dc.date_actual 
    where date_time::date = '{{ds}}'
);

insert into mart.f_sales (date_id, item_id, customer_id , city_id, quantity , payment_amount, status)
select dc.date_id, item_id, customer_id, city_id, quantity, 
	   case 
	   		when status = 'refunded' then (-1) * payment_amount
	   		else payment_amount
	   end as payment_amount, 
	   status
from staging.user_order_log uol
join mart.d_calendar dc 							-- убрал left join т.к. если будут даты, которых нет в календаре 
	on uol.date_time::date = dc.date_actual			-- то в витрину подтянутся пустые date_id и если надо будет перезагрузить данные 
where uol.date_time::date = '{{ds}}';				-- за этот день, то эти date_id не найдутся и данные задублируются
													-- чтобы не потерять данные надо организовать какую-то проверку, но это уже для другого задания) 		