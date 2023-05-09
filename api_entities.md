### Проектирование

Витрина с данными по выплатам курьерам

 

| Поле | Таблица DDS | API | Поле источника |
|------|-------------|-----|----------------|

|courier_id|fct_deliveries|couriers|_id
|courier_name|fct_deliveries|couriers|name
|settlement_year|fct_deliveries|deliveries|delivery_ts
|settlement_month|fct_deliveries|deliveries|delivery_ts
|orders_count|dm_orders	
|orders_total_sum|fct_deliveries|deliveries|sum
|rate_avg|fct_deliveries|deliveries|rate
|order_processing_fee|fct_deliveries|deliveries|sum
|courier_order_sum|fct_deliveries|deliveries|sum 
|courier_tips_sum|fct_deliveries|deliveries|tip_sum
|courier_reward_sum|fct_deliveries|deliveries|sum, tip_sum

