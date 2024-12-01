with
    source_categorias as (
        select *
        from {{ source('erp', 'category')}}
    )

    , renomeado as (
        select
            cast(ID as int) as pk_categoria
            , cast(CATEGORYNAME as string) as nome_categoria
            , cast(DESCRIPTION as string) as descricao_categoria
        from source_categorias
    )

select *
from renomeado