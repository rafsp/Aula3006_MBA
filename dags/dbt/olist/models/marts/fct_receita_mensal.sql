with base as ( select * from {{ ref('int_pedidos_itens') }} )
select
    date_trunc('month', data_compra) as mes,
    count(distinct pedido_id)        as total_pedidos,
    round(sum(preco), 2)             as receita
from base group by 1 order by 1
