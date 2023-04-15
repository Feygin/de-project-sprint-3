-- добавляем поле status в staging 
alter table staging.user_order_log 
add column status varchar(25);

-- добавляем поле status в хранилище
-- оно пнадобится для расчета витрины
alter table mart.f_sales
add column status varchar(25);