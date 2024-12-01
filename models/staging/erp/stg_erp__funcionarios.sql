with
    source_funcionario as (
        select *
        from {{ source('erp', 'employees')}}
    )

    , renomeado_funcionario as (
        select
            cast(ID as int) as pk_funcionario
            , cast(REPORTSTO as int) as fk_gerente
            , FIRSTNAME || ' ' || LASTNAME as nome_funcionario
            , cast(TITLE as varchar) as cargo_funcionario
            , cast (BIRTHDATE as date) as dt_nascimento_funcionario
            , cast (HIREDATE as date) as dt_contratacao
            , cast (CITY as varchar) as cidade_funcionario
            , cast (REGION as varchar) as regiao_funcionario
            , cast (COUNTRY as varchar) as pais_funcionario
        from source_funcionario
    )

select *
from renomeado_funcionario