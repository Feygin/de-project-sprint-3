-- удаляем из staging записи для текущей даты
-- так мы не задублируем данные в случае 
-- перезаливки данных за прошлый период
delete from staging.user_order_log 
where date_time::date = '{{ds}}';