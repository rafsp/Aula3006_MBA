with base as ( select * from {{ ref('int_pedidos_itens') }} )
select
    estado,
    count(distinct pedido_id)        as total_pedidos,
    round(sum(preco), 2)             as receita,
    round(avg(dias_entrega), 1)      as media_dias_entrega,
    round(100.0 * avg(atrasado), 2)  as pct_atraso
from base group by estado order by receita desc
