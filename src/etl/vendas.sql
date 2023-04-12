-- Databricks notebook source
SELECT *

FROM silver.olist.pedido as t1
LEFT JOIN silver.olist.item_pedido as t2
ON t1.idpedido = t2.idPedido

WHERE t1.dtPedido < '2018-01-01'
AND t1.dtPedido >= add_months('2018-01-01', -6)
AND t2.idVendedor IS NOT NULL
