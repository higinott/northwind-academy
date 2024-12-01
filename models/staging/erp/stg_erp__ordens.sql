with
    source_ordens as (
        select *
        from {{ source('erp', 'orders')}}
    )

    , renomeado_ordens as (
        select
            cast(ID as int) pk_pedido
            , cast(EMPLOYEEID as int) as fk_funcionario
            , cast(CUSTOMERID as string) as fk_cliente
            , cast(SHIPVIA as int) as fk_transportadora
            , cast(id as int) as numero_pedido
            , cast(ORDERDATE as date) as data_do_pedido
            , cast(SHIPPEDDATE as date) as data_do_envio
            , cast(REQUIREDDATE as date) as data_requerida_entrega
            , cast(FREIGHT as numeric) as frete
            , cast(SHIPNAME as string) as nm_destinatario
            , cast(SHIPCITY as string) as cidade_destinatario
            , cast(SHIPREGION as string) as regiao_destinatario
            , cast(SHIPCOUNTRY as string) as pais_destinatario
        from source_ordens
    )

select *
from renomeado_ordens