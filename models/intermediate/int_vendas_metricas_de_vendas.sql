with
    ordens as (
        select *
        from {{ref('stg_erp__ordens')}}
    )
    
    , ordem_itens as (
        select *
        from {{ref('stg_erp__ordem_itens')}}
    )

    , joined as (
        select
            ordem_itens.PK_PEDIDO_ITEM
            , ordem_itens.FK_PEDIDO
            , ordem_itens.FK_PRODUTO
            , ordens.FK_FUNCIONARIO
            , ordens.FK_CLIENTE
            , ordens.FK_TRANSPORTADORA
            , ordens.DATA_DO_PEDIDO
            , ordens.DATA_DO_ENVIO
            , ordens.DATA_REQUERIDA_ENTREGA
            , ordem_itens.DESCONTO_PERC
            , ordem_itens.PRECO_DA_UNIDADE
            , ordem_itens.QUANTIDADE
            , ordens.FRETE
            , ordens.NUMERO_PEDIDO
            , ordens.NM_DESTINATARIO
            , ordens.CIDADE_DESTINATARIO
            , ordens.REGIAO_DESTINATARIO
            , ordens.PAIS_DESTINATARIO
        from ordem_itens
        inner join ordens on ordem_itens.fk_pedido = ordens.pk_pedido
    )

    , metricas as (
        select
            DESCONTO_PERC
            , QUANTIDADE
            , FRETE
            , PRECO_DA_UNIDADE * QUANTIDADE as total_bruto
            , PRECO_DA_UNIDADE * (1 - DESCONTO_PERC) * QUANTIDADE as total_liquido
            , cast(
                (FRETE / COUNT(*) over(partition by NUMERO_PEDIDO))
                 as numeric(18,2)) as frete_rateado
            , case
                when DESCONTO_PERC > 0 then true
                else false
            end as teve_desconto
        from joined
    )

select * 
from metricas