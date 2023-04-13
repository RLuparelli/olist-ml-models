WITH tb_pedido_item AS(
SELECT t2.*,
       t1.dtPedido

FROM silver.olist.pedido as t1
LEFT JOIN silver.olist.item_pedido as t2
ON t1.idpedido = t2.idPedido

WHERE t1.dtPedido < '{date}'
AND t1.dtPedido >= add_months('{date}', -6)
AND t2.idVendedor IS NOT NULL

),
tb_summary as (
SELECT 
      idVendedor,
      count(distinct idPedido) AS qtdPedidos,
      count(distinct date(dtPedido)) AS qtdDias,
      count(idProduto) AS qtsItens,
--       Dias Sem Vender -> diferenca da data de hoje e a data da venda
       datediff('{date}',max(dtPedido)) AS qtdRecencia,
--       Ticket Medio
       sum(vlPreco) / count(distinct idPedido) as avgTicket,
       avg(vlPreco) AS avgValorProduto,
       max(vlPreco) AS maxValorProduto,
       min(vlPreco) AS minValorProduto,
       count(idProduto) / count(distinct idPedido) as avgProdutoPedido


FROM tb_pedido_item
GROUP BY idVendedor

),

tb_pedido_summary as(
SELECT idVendedor,
       idPedido,       
       sum(vlPreco) as vlPreco

FROM tb_pedido_item
GROUP BY idVendedor, idPedido

),

tb_min_max as(

  SELECT idVendedor,
         min(vlPreco) as minVlPreco,
         max(vlPreco) as maxVlPreco

  FROM tb_pedido_summary
  GROUP BY idVendedor
),

tb_life as (
SELECT t2.idVendedor,
       sum(vlPreco) AS LTV,
       max(datediff('{date}', dtPedido)) AS qtdeDiasBase

FROM silver.olist.pedido as t1

LEFT JOIN silver.olist.item_pedido as t2
ON t1.idpedido = t2.idPedido

WHERE t1.dtPedido < '{date}'
AND t2.idVendedor IS NOT NULL

GROUP BY t2.idVendedor
),

tb_dtPedido AS (
SELECT distinct idVendedor,
       date(dtPedido) as dtPedido

FROM tb_pedido_item
ORDER BY 1,2

),

tb_lag as (

SELECT *,
       LAG(dtPedido) OVER (PARTITION BY idVendedor ORDER BY dtPedido) AS lag1

FROM tb_dtPedido
),

tb_intervalo AS (
SELECT idVendedor,
       avg(datediff(dtPedido, lag1)) as avgIntervaloVendas
       
FROM tb_lag

GROUP BY idVendedor
)

SELECT 
       '{date}' as dtReference,
       NOW() as dtIngestion,
       t1.*,
       t2.minVlPreco,
       t2.maxVlPreco,
       -- t3.vlPreco,
       t4.LTV,
       t4.qtdeDiasBase,
       t5.avgIntervaloVendas

FROM tb_summary as t1

LEFT JOIN tb_min_max as t2
ON t1.idvendedor = t2.idVendedor

-- LEFT JOIN tb_pedido_summary as t3
-- ON t1.idvendedor = t3.idVendedor

LEFT JOIN tb_life as t4
ON t1.idvendedor = t4.idVendedor

LEFT JOIN tb_intervalo as t5
ON t1.idvendedor = t5.idVendedor