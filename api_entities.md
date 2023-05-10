### Проектирование

Витрина с данными по выплатам курьерам

 

| Поле                 | Таблица DDS (факты)    | Таблица DDS (измерения) | API      | Поле источника |
|----------------------|------------------------|-------------------------|----------|----------------|
| courier_id           | fct_deliveries | dm_couriers  |couriers     | _id            |
| courier_name         | fct_deliveries | dm_couriers  |couriers     | name           |
| settlement_year      | fct_deliveries | dm_deliveries| deliveries  | delivery_ts    |
| settlement_month     | fct_deliveries | dm_deliveries|deliveries   | delivery_ts    |
| orders_count         |                | dm_orders    |             |                |
| orders_total_sum     | fct_deliveries | dm_deliveries| deliveries  | sum            |
| rate_avg             | fct_deliveries | dm_deliveries| deliveries  | rate           |
| order_processing_fee | fct_deliveries | dm_deliveries| deliveries  | sum            |
| courier_order_sum    | fct_deliveries | dm_deliveries| deliveries  | sum            |
| courier_tips_sum     | fct_deliveries | dm_deliveries|  deliveries | tip_sum        |
| courier_reward_sum   | fct_deliveries | dm_deliveries|  deliveries | tip_sum, sum   |

