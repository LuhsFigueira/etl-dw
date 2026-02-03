-- =====================================================
-- ANALYTICS QUERIES - DATA WAREHOUSE
-- Banco: PostgreSQL
-- Objetivo: Análises financeiras e comportamentais
-- =====================================================

-- =========================
-- 1. FATURAMENTO DIÁRIO
-- =========================
SELECT
    transaction_date::date AS dia,
    SUM(final_price)       AS faturamento_total
FROM fact_transacao
GROUP BY transaction_date::date
ORDER BY dia;


-- =========================
-- 2. FATURAMENTO MENSAL
-- =========================
SELECT
    DATE_TRUNC('month', transaction_date) AS mes,
    SUM(final_price)                      AS faturamento_total
FROM fact_transacao
GROUP BY mes
ORDER BY mes;


-- =========================
-- 3. TICKET MÉDIO (POR VENDA)
-- =========================
SELECT
    AVG(valor_venda) AS ticket_medio
FROM (
    SELECT
        sale_id,
        SUM(final_price) AS valor_venda
    FROM fact_transacao
    GROUP BY sale_id
) t;


-- =========================
-- 4. PRODUTOS MAIS VENDIDOS (QUANTIDADE)
-- =========================
SELECT
    p.product_name,
    SUM(f.quantity) AS total_vendido
FROM fact_transacao f
JOIN dim_produto p
  ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_vendido DESC
LIMIT 10;


-- =========================
-- 5. PRODUTOS QUE MAIS FATURAM
-- =========================
SELECT
    p.product_name,
    SUM(f.final_price) AS faturamento
FROM fact_transacao f
JOIN dim_produto p
  ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY faturamento DESC
LIMIT 10;


-- =========================
-- 6. TOTAL DE DESCONTOS APLICADOS
-- =========================
SELECT
    SUM(total_price - final_price) AS valor_total_descontos
FROM fact_transacao;


-- =========================
-- 7. DESCONTO MÉDIO (%)
-- =========================
SELECT
    AVG(discount_pct) AS desconto_medio_pct
FROM fact_transacao
WHERE discount_pct > 0;


-- =========================
-- 8. TOP CLIENTES (VALOR GASTO)
-- =========================
SELECT
    u.name,
    u.email,
    SUM(f.final_price) AS total_gasto
FROM fact_transacao f
JOIN dim_usuario u
  ON f.user_id = u.user_id
GROUP BY u.name, u.email
ORDER BY total_gasto DESC
LIMIT 10;


-- =========================
-- 9. FATURAMENTO POR PAÍS
-- =========================
SELECT
    u.country,
    SUM(f.final_price) AS faturamento
FROM fact_transacao f
JOIN dim_usuario u
  ON f.user_id = u.user_id
GROUP BY u.country
ORDER BY faturamento DESC;


-- =========================
-- 10. FATURAMENTO POR ESTADO
-- =========================
SELECT
    u.state,
    SUM(f.final_price) AS faturamento
FROM fact_transacao f
JOIN dim_usuario u
  ON f.user_id = u.user_id
GROUP BY u.state
ORDER BY faturamento DESC;


-- =========================
-- 11. EVOLUÇÃO DE FATURAMENTO POR USUÁRIO (MENSAL)
-- =========================
SELECT
    u.name,
    DATE_TRUNC('month', f.transaction_date) AS mes,
    SUM(f.final_price) AS faturamento
FROM fact_transacao f
JOIN dim_usuario u
  ON f.user_id = u.user_id
GROUP BY u.name, mes
ORDER BY u.name, mes;
