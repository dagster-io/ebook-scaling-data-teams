CREATE ROLE replication_user WITH LOGIN REPLICATION;
GRANT CREATE ON DATABASE postgres TO replication_user;

CREATE SCHEMA IF NOT EXISTS data;

CREATE TABLE data.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE
);

CREATE TABLE data.payments (
    payment_id SERIAL PRIMARY KEY,
    amount DECIMAL(10, 2),
    payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE data.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES data.customers(customer_id),
    payment_id INTEGER REFERENCES data.payments(payment_id),
    quantity INTEGER DEFAULT 1,
    total_amount DECIMAL(10, 2),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO data.customers (first_name, last_name, email) VALUES
    ('Alice', 'Johnson', 'alice.johnson@example.com'),
    ('Bob', 'Smith', 'bob.smith@example.com'),
    ('Charlie', 'Lee', 'charlie.lee@example.com');

INSERT INTO data.payments (amount, payment_date) VALUES
    (51.98, '2024-05-01 10:30:00'),
    (89.50, '2024-05-02 14:15:00'),
    (39.95, '2024-05-03 09:00:00'),
    (25.99, '2024-05-03 11:45:00');

INSERT INTO data.orders (customer_id, payment_id, quantity, total_amount, order_date) VALUES
    (1, 1, 2, 51.98, '2024-05-01 10:30:00'),
    (2, 2, 1, 89.50, '2024-05-02 14:15:00'),
    (1, 3, 1, 39.95, '2024-05-03 09:00:00'),
    (3, 1, 1, 25.99, '2024-05-03 11:45:00'),
    (1, 4, 1, 50.99, '2024-05-03 13:45:00'),
    (2, 2, 2, 179.00, '2024-05-03 15:00:00'),
    (3, 3, 1, 25.99, '2024-05-04 16:15:00');