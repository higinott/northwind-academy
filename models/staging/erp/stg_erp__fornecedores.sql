with
    source_fornecedores as (
        select *
        from {{ source('erp', 'suppliers')}}
    )

    , renomeado_fornecedores as (
        select
            cast(ID as int) as pk_fornecedor
            , cast(COMPANYNAME as string) as nome_companhia
            , cast(CONTACTNAME as string) as nome_fornecedor
            , cast(CITY as string) as cidade_fornecedor
            , cast(COUNTRY as string) as pais_fornecedor
        from source_fornecedores
    )

select *
from renomeado_fornecedores