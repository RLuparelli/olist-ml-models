WITH tb_pedido AS (
    SELECT t1.idPedido,
           t2.idVendedor,
           t1.descSituacao,
           t1.dtPedido,
           t1.dtAprovado,
           t1.dtEntregue,
           t1.dtEstimativaEntrega,
           sum(vlFrete) as totalFrete


    FROM silver.olist.pedido AS t1

    LEFT JOIN silver.olist.item_pedido AS t2
    ON t1.idPedido = t2.idPedido

    WHERE t1.dtPedido < '{date}'
    AND t1.dtPedido >= add_months('{date}', -6)
    AND t2.idVendedor IS NOT NULL

    GROUP BY t1.idPedido,
           t2.idVendedor,
           t1.descSituacao,
           t1.dtPedido,
           t1.dtAprovado,
           t1.dtEntregue,
           t1.dtEstimativaEntrega
       
),
tb_summary as(
SELECT 
      idVendedor,
      count(distinct case when date(coalesce(dtEntregue, '{date}')) >
      date(dtEstimativaEntrega) then idPedido end) / count(distinct case when descSituacao =
      'delivered' THEN idPedido END) AS pctPedidoAtrado,
       count(distinct case when descSituacao = 'canceled' then idPedido end) / count(distinct idPedido) AS pctPedidoCancelado,
       avg(totalFrete) as avgFrete,
       percentile(totalFrete,0.5) as medianFrete,
       max(totalFrete) as maxFrete,
       min(totalFrete) as minFrete,
--        Tempo Medio de Entrega
       avg(datediff(coalesce(dtEntregue, '{date}'), dtAprovado)) AS qtdDiasAprovadoEntrega,
       avg(datediff(coalesce(dtEntregue, '{date}'), dtPedido)) AS qtdDiasPedidoEntrega,
       avg(datediff(dtEstimativaEntrega,coalesce(dtEntregue, '{date}'))) AS qtdtDiasEntregaPromessa


FROM tb_pedido


GROUP BY 1

)

SELECT  '{date}' as dtReference,
        NOW() as dtIngestion,
        *

FROM tb_summary