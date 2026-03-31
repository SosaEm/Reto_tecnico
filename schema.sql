CREATE DATABASE IF NOT EXISTS CODEABLE;
USE CODEABLE;

-- `dim_users` - dimensión usuarios
CREATE TABLE IF NOT EXISTS dim_users (
    user_id VARCHAR(50) PRIMARY KEY,
    country_code VARCHAR(10)
);

-- `dim_merchants` - dimensión comercios
CREATE TABLE IF NOT EXISTS dim_merchants (
    merchant_id VARCHAR(50) PRIMARY KEY,
    merchant_name VARCHAR(100)
);

-- `dim_time` - dimensión temporal
CREATE TABLE IF NOT EXISTS dim_time (
    time_id VARCHAR(20) PRIMARY KEY, -- Formato YYYYMMDDHHmm
    date DATE,
    hour INT,
    day_of_week VARCHAR(15)
);

-- `dim_payment_methods` - dimensión métodos de pago
CREATE TABLE IF NOT EXISTS dim_payment_methods (
    payment_method_id INT AUTO_INCREMENT PRIMARY KEY,
    method_name VARCHAR(50) UNIQUE -- Ej: 'credit_card', 'transfer', 'crypto'
);

-- `fact_transactions` - tabla de hechos
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    merchant_id VARCHAR(50),
    time_id VARCHAR(20),
    payment_method_id INT,
    amount DECIMAL(18, 2),
    currency VARCHAR(10),
    status VARCHAR(20),
    FOREIGN KEY (user_id) REFERENCES dim_users(user_id),
    FOREIGN KEY (merchant_id) REFERENCES dim_merchants(merchant_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (payment_method_id) REFERENCES dim_payment_methods(payment_method_id)
);