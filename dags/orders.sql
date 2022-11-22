with details_order as(
  select DATE(order_time, 'Asia/Jakarta') AS order_date,
    struct(
    order_type,
    count(customer_no) as total_customer_per_order_type
  ) as details
  from `{{ params.project_id }}.source.daily_order` where order_status = 'Completed' group by order_type, order_date
) ,
main_order as(
  select DATE(order_time, 'Asia/Jakarta') AS order_date,
  count(customer_no) as total_customer,
  order_payment,
  from `{{ params.project_id }}.source.daily_order` where order_status = 'Completed' group by order_date, order_payment
)
select m.order_date as order_date,
m.total_customer as total_customer,
d.details as details,
m.order_payment as order_payment
from main_order as m 
left join details_order d on d.order_date = m.order_date
where m.order_date between @start_date and @end_date
