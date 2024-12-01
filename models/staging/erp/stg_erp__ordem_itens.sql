with
    source_ordens_itens as (
        select *
        from {{ source('erp', 'order_details')}}
    )

    , renomeado_ordens_itens as (
        select
            ORDERID::varchar || '-' || PRODUCTID::varchar as pk_pedido_item
            , cast(ORDERID as int) as fk_pedido
            , cast(PRODUCTID as int) as fk_produto
            , cast(DISCOUNT as numeric(18,2)) as desconto_perc
            , cast(UNITPRICE as numeric(18,2)) as preco_da_unidade
            , cast(QUANTITY as int) as quantidade
        from source_ordens_itens
    )

select *
from renomeado_ordens_itens