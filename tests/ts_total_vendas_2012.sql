/*
    Este teste garante que as vendas brutas de 2012 estão
    corretas com o valor auditado da contabilidade:
    R$ 230,784.68
*/

with metricas_vendas as (
    select *
    from {{ref('int_vendas_metricas_de_vendas')}}
),

vendas_em_2012 as (
    select sum(total_bruto) as soma_total_bruto
    from metricas_vendas
    where data_do_pedido between '2012-01-01' and '2012-12-31'
)

select soma_total_bruto
from vendas_em_2012
where soma_total_bruto not between 230784.68 and 230785.00