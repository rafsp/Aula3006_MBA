with pedidos as ( select * from {{ ref('stg_olist__pedidos') }} ),
     itens   as ( select * from {{ ref('stg_olist__itens') }} ),
     clientes as ( select * from {{ ref('stg_olist__clientes') }} )
select
    p.pedido_id, p.status, c.estado, i.preco, i.frete,
    datediff('day', p.data_compra, p.data_entrega)  as dias_entrega,
    case when p.data_entrega > p.data_estimada
         then 1 else 0 end                          as atrasado
from pedidos p
join itens i      on p.pedido_id = i.pedido_id
left join clientes c on p.cliente_id = c.cliente_id
where p.status = 'delivered'
