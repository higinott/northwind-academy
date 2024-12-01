with
    fact_vendas as (
        select *
        from {{ ref('int_vendas_metricas_de_vendas')}}
    )

select *
from fact_vendas