with
    categorias as (
        select *
        from {{ref('stg_erp__categorias')}}
    )
    
    , fornecedores as (
        select *
        from {{ref('stg_erp__fornecedores')}}
    )

    , produtos as (
        select *
        from {{ref('stg_erp__produtos')}}
    )

    , enriquecer_produtos as (
        select
            produtos.pk_produtos
            , produtos.NM_PRODUTO
            , produtos.quantidade_por_unidade
            , produtos.unidade_em_estoque
            , produtos.preco_por_unidade
            , produtos.unidade_por_pedido
            , produtos.nivel_de_pedido
            , produtos.eh_discontinuado
            , fornecedores.NOME_COMPANHIA
            , fornecedores.NOME_FORNECEDOR
            , fornecedores.CIDADE_FORNECEDOR
            , fornecedores.PAIS_FORNECEDOR
            , categorias.NOME_CATEGORIA
            , categorias.DESCRICAO_CATEGORIA
        from produtos
        left join categorias on categorias.pk_categoria = produtos.fk_categoria
        left join fornecedores on fornecedores.pk_fornecedor = produtos.fk_fornecedor
    )

select *
from enriquecer_produtos