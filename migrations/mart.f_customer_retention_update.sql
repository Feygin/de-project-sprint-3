/* удаляем из витрины данные за текущую неделю */
delete from mart.f_customer_retention fcr
using mart.d_calendar dc
where (string_to_array(fcr.period_id, '-'))[1]::varchar(4) = dc.year_actual::varchar(4)		-- парсим период обратно на составляющие
	and (string_to_array(fcr.period_id, '-'))[2]::varchar(2) = dc.week_of_year::varchar(2)	-- джойним с календарем
	and '{{ds}}' between dc.first_day_of_week and dc.last_day_of_week;

/* загружаем витрину инкрментально для текущей недели */
-- добавляем поле с номером недели
-- и оставляем данные за текущую неделю
with add_weekly_period as (
	select
		 concat_ws('-', dc.year_actual::varchar(4), dc.week_of_year::varchar(2)) as period_id
		,item_id 
		,customer_id
		,status
		,quantity
		,payment_amount 
	from mart.f_sales s
	join mart.d_calendar dc 
		on s.date_id = dc.date_id
	where '{{ds}}' between dc.first_day_of_week and dc.last_day_of_week
),

-- новые покупатели за период
new_customers as (
select period_id, customer_id
from add_weekly_period
where status = 'shipped'
group by period_id, customer_id
having count(*) = 1				-- одна покупка за период
),

-- вернувшиеся покупатели за период
returning_customers as (
select period_id, customer_id
from add_weekly_period
where status = 'shipped'
group by period_id, customer_id
having count(*) > 1				-- более одной покупки за период
),

-- покупатели, которые вернули товар
-- этот маркер нам понадобится чтобы посчитать
-- уникальное кол-во покупателей, которые вернули товар 
-- так как case нельза вкладывать внутрь distinct
refunding_customers as (
select period_id, item_id, customer_id
from add_weekly_period
where status = 'refunded'
group by period_id, item_id, customer_id	-- смотрим в разрезе товара
),

-- объединяем данные, считаем метрики
combine_everything as (
select
	 count(distinct n.customer_id)  									as new_customers_count
	,count(distinct rt.customer_id) 									as returning_customers_count
	,count(distinct rf.customer_id)										as refunding_customers_count
	,'weekly'															as period_name
	,s.period_id														as period_id
	,s.item_id															as item_id
	,sum(case when n.customer_id is not null then payment_amount end) 	as new_customers_revenue
	,sum(case when rt.customer_id is not null then payment_amount end) 	as returning_customers_revenue
	,sum(case when rf.customer_id is not null then quantity end) 		as customers_refund
from add_weekly_period 	s
left join new_customers n on
	 	s.period_id   = n.period_id
	and s.customer_id = n.customer_id
left join returning_customers rt on
		s.period_id   = rt.period_id
	and	s.customer_id = rt.customer_id
left join refunding_customers rf on										-- не забываем, что возврат товара
		s.period_id	  = rf.period_id									-- осуществляется по гранулярности товара
	and s.item_id     = rf.item_id
	and	s.customer_id = rf.customer_id
group by
	 s.period_id
	,s.item_id
)

insert into mart.f_customer_retention (
	new_customers_count, returning_customers_count, refunding_customers_count,
	period_name, period_id, item_id, new_customers_revenue, returning_customers_revenue,
	customers_refund
)
select * from combine_everything;