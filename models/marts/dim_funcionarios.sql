with
    dim_funcinarios as (
        select *
        from {{ ref('int_vendas_self_join_funcionarios')}}
    )

select *
from dim_funcinarios