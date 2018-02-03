insert overwrite table retail.top10 
-- top 10 revenue generating products
select p.product_id, p.product_name, r.revenue
from retail.products p 
    inner join (
        select oi.order_item_product_id, sum(cast(oi.order_item_subtotal as float)) as revenue
        from retail.order_items oi 
            inner join retail.orders o on oi.order_item_order_id = o.order_id
        where o.order_status <> 'CANCELED' and o.order_status <> 'SUSPECTED_FRAUD'
        group by order_item_product_id) r
    on (p.product_id = r.order_item_product_id)
order by r.revenue desc
limit 10;