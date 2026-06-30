with src as ( select * from {{ source('bronze','ORDERS') }} )
select
    ORDER_ID                          as pedido_id,
    CUSTOMER_ID                       as cliente_id,
    ORDER_STATUS                      as status,
    ORDER_PURCHASE_TIMESTAMP::timestamp        as data_compra,
    ORDER_DELIVERED_CUSTOMER_DATE::timestamp   as data_entrega,
    ORDER_ESTIMATED_DELIVERY_DATE::timestamp   as data_estimada
from src
