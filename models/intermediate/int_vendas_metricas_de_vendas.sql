-- CTE que busca os dados da tabela de ordens de pedidos
with ordens as (
    select *
    from {{ref('stg_erp__ordens')}} -- Refere-se ao modelo transformado 'stg_erp__ordens'
),

-- CTE que busca os dados da tabela de itens de pedidos
ordem_itens as (
    select *
    from {{ref('stg_erp__ordem_itens')}} -- Refere-se ao modelo transformado 'stg_erp__ordem_itens'
),

-- CTE que realiza o join entre as ordens e os itens do pedido
joined as (
    select
        ordem_itens.PK_PEDIDO_ITEM, -- Chave primária do item do pedido
        ordem_itens.FK_PEDIDO,      -- Chave estrangeira referenciando o pedido
        ordem_itens.FK_PRODUTO,     -- Chave estrangeira referenciando o produto
        ordens.FK_FUNCIONARIO,      -- Chave estrangeira referenciando o funcionário responsável pelo pedido
        ordens.FK_CLIENTE,          -- Chave estrangeira referenciando o cliente
        ordens.FK_TRANSPORTADORA,   -- Chave estrangeira referenciando a transportadora
        ordens.DATA_DO_PEDIDO,      -- Data do pedido
        ordens.DATA_DO_ENVIO,       -- Data de envio do pedido
        ordens.DATA_REQUERIDA_ENTREGA, -- Data requerida de entrega do pedido
        ordem_itens.DESCONTO_PERC,  -- Percentual de desconto aplicado no item
        ordem_itens.PRECO_DA_UNIDADE, -- Preço unitário do item
        ordem_itens.QUANTIDADE,     -- Quantidade do item no pedido
        ordens.FRETE,               -- Custo total do frete
        ordens.NUMERO_PEDIDO,       -- Número identificador do pedido
        ordens.NM_DESTINATARIO,     -- Nome do destinatário do pedido
        ordens.CIDADE_DESTINATARIO, -- Cidade do destinatário
        ordens.REGIAO_DESTINATARIO, -- Região do destinatário
        ordens.PAIS_DESTINATARIO    -- País do destinatário
    from ordem_itens
    inner join ordens on ordem_itens.fk_pedido = ordens.pk_pedido -- Realiza o join com base no ID do pedido
),

-- CTE que calcula métricas baseadas nos dados unidos
metricas as (
    select
        DATA_DO_PEDIDO,
        DESCONTO_PERC, -- Percentual de desconto aplicado no item
        QUANTIDADE,    -- Quantidade de itens no pedido
        FRETE,         -- Valor total do frete
        PRECO_DA_UNIDADE * QUANTIDADE as total_bruto, -- Valor total bruto (preço unitário * quantidade)
        PRECO_DA_UNIDADE * (1 - DESCONTO_PERC) * QUANTIDADE as total_liquido, -- Valor total líquido (aplicando desconto)
        
        -- Calcula o frete rateado por item com base no número total de itens no pedido
        cast(
            (FRETE / COUNT(*) over(partition by NUMERO_PEDIDO))
            as numeric(18,2)
        ) as frete_rateado,

        -- Verifica se o item teve desconto (true/false)
        case
            when DESCONTO_PERC > 0 then true
            else false
        end as teve_desconto
    from joined
)

-- Seleciona as métricas calculadas
select * 
from metricas
