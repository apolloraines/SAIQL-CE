-- MySQL Test Database Initialization
-- ==================================

-- Set SQL mode for strict compliance
SET sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO';

-- Use the test database
USE saiql_test;

-- Users table for testing
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    age INT CHECK (age > 0 AND age < 150),
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    metadata JSON
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Products table for testing
CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    category VARCHAR(100),
    stock_quantity INT DEFAULT 0 CHECK (stock_quantity >= 0),
    is_active BOOLEAN DEFAULT true,
    tags JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_category (category),
    INDEX idx_price (price),
    INDEX idx_name (name),
    FULLTEXT idx_description (description)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Orders table for testing
CREATE TABLE orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    product_id INT,
    quantity INT NOT NULL CHECK (quantity > 0),
    total_price DECIMAL(10,2) NOT NULL CHECK (total_price >= 0),
    order_status VARCHAR(50) DEFAULT 'pending',
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    shipping_address JSON,
    notes TEXT,
    INDEX idx_user_id (user_id),
    INDEX idx_product_id (product_id),
    INDEX idx_status (order_status),
    INDEX idx_date (order_date),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Logs table for testing
CREATE TABLE logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    level VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    source VARCHAR(100),
    user_id INT,
    request_id VARCHAR(36),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSON,
    INDEX idx_level (level),
    INDEX idx_timestamp (timestamp),
    INDEX idx_source (source),
    INDEX idx_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Documents table for vector/semantic testing
CREATE TABLE documents (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    content TEXT NOT NULL,
    author VARCHAR(255),
    category VARCHAR(100),
    tags JSON,
    word_count INT,
    language VARCHAR(10) DEFAULT 'en',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_category (category),
    INDEX idx_author (author),
    FULLTEXT idx_title_content (title, content)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Performance test table (large dataset)
CREATE TABLE large_dataset (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    uuid_field VARCHAR(36),
    text_field VARCHAR(1000),
    numeric_field DECIMAL(15,4),
    integer_field INT,
    boolean_field BOOLEAN,
    date_field DATE,
    timestamp_field TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    json_field JSON,
    INDEX idx_uuid (uuid_field),
    INDEX idx_numeric (numeric_field),
    INDEX idx_integer (integer_field),
    INDEX idx_date (date_field)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert test data
INSERT INTO users (username, email, password_hash, full_name, age, status, metadata) VALUES
('alice_smith', 'alice@example.com', SHA2('password123', 256), 'Alice Smith', 28, 'active', '{"preferences": {"theme": "dark"}, "last_login": "2025-08-14T10:30:00Z"}'),
('bob_jones', 'bob@example.com', SHA2('password456', 256), 'Bob Jones', 35, 'active', '{"preferences": {"theme": "light"}, "role": "admin"}'),
('charlie_brown', 'charlie@example.com', SHA2('password789', 256), 'Charlie Brown', 42, 'inactive', '{"preferences": {"notifications": false}}'),
('diana_prince', 'diana@example.com', SHA2('password321', 256), 'Diana Prince', 31, 'active', '{"preferences": {"theme": "auto"}, "department": "engineering"}'),
('eve_wilson', 'eve@example.com', SHA2('password654', 256), 'Eve Wilson', 29, 'active', '{"preferences": {"language": "es"}, "timezone": "PST"}');

INSERT INTO products (name, description, price, category, stock_quantity, tags) VALUES
('Laptop Pro 15', 'High-performance laptop for professionals', 1299.99, 'Electronics', 25, '["computers", "laptops", "professional"]'),
('Wireless Mouse', 'Ergonomic wireless mouse with precision tracking', 49.99, 'Electronics', 150, '["accessories", "mouse", "wireless"]'),
('Office Chair', 'Comfortable ergonomic office chair', 299.99, 'Furniture', 45, '["furniture", "office", "ergonomic"]'),
('Coffee Maker', 'Programmable coffee maker with timer', 89.99, 'Appliances', 30, '["kitchen", "coffee", "appliances"]'),
('Notebook Set', 'Premium notebook set for professionals', 24.99, 'Office Supplies', 200, '["notebooks", "writing", "office"]'),
('Standing Desk', 'Adjustable height standing desk', 499.99, 'Furniture', 15, '["furniture", "desk", "ergonomic", "standing"]'),
('Bluetooth Headphones', 'Noise-cancelling bluetooth headphones', 199.99, 'Electronics', 75, '["audio", "headphones", "bluetooth", "noise-cancelling"]'),
('Smartphone Case', 'Protective case for smartphones', 19.99, 'Electronics', 300, '["accessories", "phone", "protection"]');

INSERT INTO orders (user_id, product_id, quantity, total_price, order_status, shipping_address, notes) VALUES
(1, 1, 1, 1299.99, 'completed', '{"street": "123 Main St", "city": "San Francisco", "state": "CA", "zip": "94105"}', 'Express shipping requested'),
(1, 2, 2, 99.98, 'completed', '{"street": "123 Main St", "city": "San Francisco", "state": "CA", "zip": "94105"}', 'Same address as previous order'),
(2, 3, 1, 299.99, 'shipped', '{"street": "456 Oak Ave", "city": "Portland", "state": "OR", "zip": "97201"}', 'Delivery instructions: leave at door'),
(3, 4, 1, 89.99, 'pending', '{"street": "789 Pine St", "city": "Seattle", "state": "WA", "zip": "98101"}', 'Gift wrapping requested'),
(4, 5, 5, 124.95, 'completed', '{"street": "321 Elm St", "city": "Austin", "state": "TX", "zip": "78701"}', 'Bulk order for team'),
(5, 6, 1, 499.99, 'processing', '{"street": "654 Maple Dr", "city": "Denver", "state": "CO", "zip": "80202"}', 'Assembly service requested');

INSERT INTO logs (level, message, source, user_id, request_id, metadata) VALUES
('INFO', 'User login successful', 'auth_service', 1, UUID(), '{"ip": "192.168.1.100", "user_agent": "Mozilla/5.0 Chrome/91.0"}'),
('ERROR', 'Database connection timeout', 'db_service', NULL, UUID(), '{"timeout": 30, "retry_count": 3}'),
('WARN', 'High memory usage detected', 'system_monitor', NULL, UUID(), '{"memory_percent": 85, "threshold": 80}'),
('INFO', 'Order created successfully', 'order_service', 2, UUID(), '{"order_id": 1, "total": 1299.99}'),
('DEBUG', 'Cache miss for user preferences', 'cache_service', 1, UUID(), '{"cache_key": "user_prefs_1", "ttl": 3600}'),
('ERROR', 'Payment processing failed', 'payment_service', 3, UUID(), '{"error_code": "CARD_DECLINED", "amount": 89.99}'),
('INFO', 'Product inventory updated', 'inventory_service', NULL, UUID(), '{"product_id": 2, "old_stock": 150, "new_stock": 148}');

INSERT INTO documents (title, content, author, category, tags, word_count, language) VALUES
('Introduction to Machine Learning', 'Machine learning is a subset of artificial intelligence that focuses on algorithms that can learn from and make predictions on data. This comprehensive guide covers the fundamentals of supervised and unsupervised learning, neural networks, and deep learning techniques.', 'Dr. Sarah Johnson', 'Technology', '["AI", "ML", "tutorial"]', 42, 'en'),
('Sustainable Agriculture Practices', 'Modern sustainable agriculture combines traditional farming wisdom with innovative technologies to create resilient food systems. This article explores permaculture principles, crop rotation strategies, and the role of biodiversity in maintaining healthy ecosystems.', 'Prof. Michael Green', 'Agriculture', '["sustainability", "farming", "environment"]', 38, 'en'),
('Quantum Computing Fundamentals', 'Quantum computing represents a paradigm shift in computational power, leveraging quantum mechanical phenomena like superposition and entanglement. This technical overview discusses qubits, quantum gates, and potential applications in cryptography and optimization.', 'Dr. Lisa Chen', 'Technology', '["quantum", "computing", "physics"]', 35, 'en'),
('Climate Change Adaptation Strategies', 'As global temperatures rise and weather patterns shift, communities worldwide must develop robust adaptation strategies. This policy paper examines successful case studies in coastal protection, water management, and urban planning for climate resilience.', 'Climate Research Institute', 'Environment', '["climate", "adaptation", "policy"]', 41, 'en'),
('Blockchain in Supply Chain Management', 'Blockchain technology offers unprecedented transparency and traceability in global supply chains. This analysis covers smart contracts, distributed ledger benefits, and real-world implementations in food safety, pharmaceuticals, and luxury goods authentication.', 'Tech Innovation Labs', 'Technology', '["blockchain", "supply-chain", "transparency"]', 36, 'en');

-- Create stored procedure for testing
DELIMITER $$

CREATE PROCEDURE GetUserOrderSummary(IN user_id_param INT)
BEGIN
    SELECT 
        u.username,
        COUNT(o.id) as total_orders,
        COALESCE(SUM(o.total_price), 0) as total_spent,
        MAX(o.order_date) as last_order_date
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.id = user_id_param
    GROUP BY u.id, u.username;
END$$

DELIMITER ;

-- Generate larger dataset for performance testing
-- This needs to be done in batches for MySQL
INSERT INTO large_dataset (uuid_field, text_field, numeric_field, integer_field, boolean_field, date_field, json_field)
SELECT 
    UUID(),
    CONCAT('Performance test record ', (@row_number := @row_number + 1), ' with random data for testing database operations'),
    ROUND(RAND() * 10000, 4),
    FLOOR(RAND() * 1000000),
    RAND() > 0.5,
    DATE_SUB(CURRENT_DATE, INTERVAL FLOOR(RAND() * 365) DAY),
    JSON_OBJECT(
        'id', @row_number,
        'category', ELT(FLOOR(RAND() * 5 + 1), 'A', 'B', 'C', 'D', 'E'),
        'score', ROUND(RAND() * 100, 2),
        'active', RAND() > 0.3
    )
FROM 
    (SELECT @row_number := 0) r,
    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t1,
    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t2,
    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t3,
    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t4
LIMIT 10000;

-- Optimize tables
OPTIMIZE TABLE users;
OPTIMIZE TABLE products;
OPTIMIZE TABLE orders;
OPTIMIZE TABLE logs;
OPTIMIZE TABLE documents;
OPTIMIZE TABLE large_dataset;
-- IHNpZ25hdHVyZTplYzUyNTBkZWRmNTZjZTZiMDA4NCBpZDpTQUlRTC1DSEFSTElFLVBST0QgYnk6QXBvbGxvICYgQ2xhdWRlIA==
