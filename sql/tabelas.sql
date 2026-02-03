CREATE TABLE dim_usuario (
    user_id        INT PRIMARY KEY,
    name           VARCHAR(150),
    email          VARCHAR(150),
    gender         VARCHAR(20),
    birthdate      DATE,
    city           VARCHAR(100),
    state          VARCHAR(100),
    country        VARCHAR(100)
);







CREATE TABLE dim_produto (
    product_id    INT PRIMARY KEY,
    sku           VARCHAR(100),
    product_name  VARCHAR(200),
    category      VARCHAR(100),
    brand         VARCHAR(100),
    price         NUMERIC(10,2),
    rating        NUMERIC(4,2),
    stock         INT,
    barcode       VARCHAR(50),
    created_at    DATE
    
);



CREATE TABLE fact_transacao (
    sale_id        INT,
    user_id        INT,
    product_id     INT,
    transaction_date DATE,
    quantity       INT,
    unit_price     NUMERIC(10,2),
    total_price    NUMERIC(10,2),
    discount_pct   NUMERIC(5,2),
    final_price    NUMERIC(10,2),

    PRIMARY KEY (sale_id, product_id)



)






