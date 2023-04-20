-- добавляем поле status в staging 
alter table staging.user_order_log 
add column status varchar(25);

-- добавляем поле status в хранилище
-- оно пнадобится для расчета витрины
alter table mart.f_sales
add column status varchar(25);

-- создаем таблицу для витрины
create table mart.f_customer_retention (
	new_customers_count 			integer,
	returning_customers_count 		integer,
	refunding_customers_count 		integer,
	period_name						char(6),
	period_id						integer not null,
	item_id							integer not null,
	new_customers_revenue			numeric(14,2),
	returning_customers_revenue		numeric(14,2),
	customers_refund				integer
);