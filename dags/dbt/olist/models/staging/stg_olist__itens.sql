with src as ( select * from {{ source('bronze','ORDER_ITEMS') }} )
select
    ORDER_ID        as pedido_id,
    ORDER_ITEM_ID   as item_id,
    PRICE           as preco,
    FREIGHT_VALUE   as frete
from src
