-- PostgreSQL Test Database Initialization
-- ========================================

-- Create test schemas
CREATE SCHEMA IF NOT EXISTS saiql_test;
CREATE SCHEMA IF NOT EXISTS performance_test;

-- Set search path
SET search_path TO saiql_test, public;

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Users table for testing
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    age INTEGER CHECK (age > 0 AND age < 150),
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Products table for testing  
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    category VARCHAR(100),
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    is_active BOOLEAN DEFAULT true,
    tags TEXT[],
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Orders table for testing
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    total_price DECIMAL(10,2) NOT NULL CHECK (total_price >= 0),
    order_status VARCHAR(50) DEFAULT 'pending',
    order_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    shipping_address JSONB,
    notes TEXT
);

-- Logs table for testing
CREATE TABLE logs (
    id BIGSERIAL PRIMARY KEY,
    level VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    source VARCHAR(100),
    user_id INTEGER,
    request_id UUID DEFAULT gen_random_uuid(),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Documents table for vector/semantic testing
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    content TEXT NOT NULL,
    author VARCHAR(255),
    category VARCHAR(100),
    tags TEXT[],
    word_count INTEGER,
    language VARCHAR(10) DEFAULT 'en',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Performance test table (large dataset)
CREATE TABLE performance_test.large_dataset (
    id BIGSERIAL PRIMARY KEY,
    uuid_field UUID DEFAULT gen_random_uuid(),
    text_field VARCHAR(1000),
    numeric_field DECIMAL(15,4),
    integer_field INTEGER,
    boolean_field BOOLEAN,
    date_field DATE,
    timestamp_field TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    json_field JSONB,
    array_field INTEGER[]
);

-- Indexes for performance
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_status ON users(status);
CREATE INDEX idx_users_created_at ON users(created_at);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_products_name_gin ON products USING GIN(to_tsvector('english', name));
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(order_status);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_logs_level ON logs(level);
CREATE INDEX idx_logs_timestamp ON logs(timestamp);
CREATE INDEX idx_logs_metadata_gin ON logs USING GIN(metadata);
CREATE INDEX idx_documents_category ON documents(category);
CREATE INDEX idx_documents_content_gin ON documents USING GIN(to_tsvector('english', content));

-- Insert test data
INSERT INTO users (username, email, password_hash, full_name, age, status, metadata) VALUES
('alice_smith', 'alice@example.com', crypt('password123', gen_salt('bf')), 'Alice Smith', 28, 'active', '{"preferences": {"theme": "dark"}, "last_login": "2025-08-14T10:30:00Z"}'),
('bob_jones', 'bob@example.com', crypt('password456', gen_salt('bf')), 'Bob Jones', 35, 'active', '{"preferences": {"theme": "light"}, "role": "admin"}'),
('charlie_brown', 'charlie@example.com', crypt('password789', gen_salt('bf')), 'Charlie Brown', 42, 'inactive', '{"preferences": {"notifications": false}}'),
('diana_prince', 'diana@example.com', crypt('password321', gen_salt('bf')), 'Diana Prince', 31, 'active', '{"preferences": {"theme": "auto"}, "department": "engineering"}'),
('eve_wilson', 'eve@example.com', crypt('password654', gen_salt('bf')), 'Eve Wilson', 29, 'active', '{"preferences": {"language": "es"}, "timezone": "PST"}');

INSERT INTO products (name, description, price, category, stock_quantity, tags) VALUES
('Laptop Pro 15', 'High-performance laptop for professionals', 1299.99, 'Electronics', 25, '{"computers", "laptops", "professional"}'),
('Wireless Mouse', 'Ergonomic wireless mouse with precision tracking', 49.99, 'Electronics', 150, '{"accessories", "mouse", "wireless"}'),
('Office Chair', 'Comfortable ergonomic office chair', 299.99, 'Furniture', 45, '{"furniture", "office", "ergonomic"}'),
('Coffee Maker', 'Programmable coffee maker with timer', 89.99, 'Appliances', 30, '{"kitchen", "coffee", "appliances"}'),
('Notebook Set', 'Premium notebook set for professionals', 24.99, 'Office Supplies', 200, '{"notebooks", "writing", "office"}'),
('Standing Desk', 'Adjustable height standing desk', 499.99, 'Furniture', 15, '{"furniture", "desk", "ergonomic", "standing"}'),
('Bluetooth Headphones', 'Noise-cancelling bluetooth headphones', 199.99, 'Electronics', 75, '{"audio", "headphones", "bluetooth", "noise-cancelling"}'),
('Smartphone Case', 'Protective case for smartphones', 19.99, 'Electronics', 300, '{"accessories", "phone", "protection"}');

INSERT INTO orders (user_id, product_id, quantity, total_price, order_status, shipping_address, notes) VALUES
(1, 1, 1, 1299.99, 'completed', '{"street": "123 Main St", "city": "San Francisco", "state": "CA", "zip": "94105"}', 'Express shipping requested'),
(1, 2, 2, 99.98, 'completed', '{"street": "123 Main St", "city": "San Francisco", "state": "CA", "zip": "94105"}', 'Same address as previous order'),
(2, 3, 1, 299.99, 'shipped', '{"street": "456 Oak Ave", "city": "Portland", "state": "OR", "zip": "97201"}', 'Delivery instructions: leave at door'),
(3, 4, 1, 89.99, 'pending', '{"street": "789 Pine St", "city": "Seattle", "state": "WA", "zip": "98101"}', 'Gift wrapping requested'),
(4, 5, 5, 124.95, 'completed', '{"street": "321 Elm St", "city": "Austin", "state": "TX", "zip": "78701"}', 'Bulk order for team'),
(5, 6, 1, 499.99, 'processing', '{"street": "654 Maple Dr", "city": "Denver", "state": "CO", "zip": "80202"}', 'Assembly service requested');

INSERT INTO logs (level, message, source, user_id, metadata) VALUES
('INFO', 'User login successful', 'auth_service', 1, '{"ip": "192.168.1.100", "user_agent": "Mozilla/5.0 Chrome/91.0"}'),
('ERROR', 'Database connection timeout', 'db_service', NULL, '{"timeout": 30, "retry_count": 3}'),
('WARN', 'High memory usage detected', 'system_monitor', NULL, '{"memory_percent": 85, "threshold": 80}'),
('INFO', 'Order created successfully', 'order_service', 2, '{"order_id": 1, "total": 1299.99}'),
('DEBUG', 'Cache miss for user preferences', 'cache_service', 1, '{"cache_key": "user_prefs_1", "ttl": 3600}'),
('ERROR', 'Payment processing failed', 'payment_service', 3, '{"error_code": "CARD_DECLINED", "amount": 89.99}'),
('INFO', 'Product inventory updated', 'inventory_service', NULL, '{"product_id": 2, "old_stock": 150, "new_stock": 148}');

INSERT INTO documents (title, content, author, category, tags, word_count, language) VALUES
('Introduction to Machine Learning', 'Machine learning is a subset of artificial intelligence that focuses on algorithms that can learn from and make predictions on data. This comprehensive guide covers the fundamentals of supervised and unsupervised learning, neural networks, and deep learning techniques.', 'Dr. Sarah Johnson', 'Technology', '{"AI", "ML", "tutorial"}', 42, 'en'),
('Sustainable Agriculture Practices', 'Modern sustainable agriculture combines traditional farming wisdom with innovative technologies to create resilient food systems. This article explores permaculture principles, crop rotation strategies, and the role of biodiversity in maintaining healthy ecosystems.', 'Prof. Michael Green', 'Agriculture', '{"sustainability", "farming", "environment"}', 38, 'en'),
('Quantum Computing Fundamentals', 'Quantum computing represents a paradigm shift in computational power, leveraging quantum mechanical phenomena like superposition and entanglement. This technical overview discusses qubits, quantum gates, and potential applications in cryptography and optimization.', 'Dr. Lisa Chen', 'Technology', '{"quantum", "computing", "physics"}', 35, 'en'),
('Climate Change Adaptation Strategies', 'As global temperatures rise and weather patterns shift, communities worldwide must develop robust adaptation strategies. This policy paper examines successful case studies in coastal protection, water management, and urban planning for climate resilience.', 'Climate Research Institute', 'Environment', '{"climate", "adaptation", "policy"}', 41, 'en'),
('Blockchain in Supply Chain Management', 'Blockchain technology offers unprecedented transparency and traceability in global supply chains. This analysis covers smart contracts, distributed ledger benefits, and real-world implementations in food safety, pharmaceuticals, and luxury goods authentication.', 'Tech Innovation Labs', 'Technology', '{"blockchain", "supply-chain", "transparency"}', 36, 'en');

-- Generate larger dataset for performance testing
INSERT INTO performance_test.large_dataset (text_field, numeric_field, integer_field, boolean_field, date_field, json_field, array_field)
SELECT 
    'Performance test record ' || generate_series || ' with random data for testing database operations',
    ROUND((random() * 10000)::numeric, 4),
    floor(random() * 1000000)::int,
    random() > 0.5,
    current_date - floor(random() * 365)::int,
    json_build_object(
        'id', generate_series,
        'category', (ARRAY['A', 'B', 'C', 'D', 'E'])[floor(random() * 5 + 1)],
        'score', round(random() * 100, 2),
        'active', random() > 0.3
    ),
    ARRAY[floor(random() * 100)::int, floor(random() * 100)::int, floor(random() * 100)::int]
FROM generate_series(1, 10000);

-- Update statistics
ANALYZE users;
ANALYZE products;  
ANALYZE orders;
ANALYZE logs;
ANALYZE documents;
ANALYZE performance_test.large_dataset;

-- Create a test function for stored procedure testing
CREATE OR REPLACE FUNCTION get_user_order_summary(user_id_param INTEGER)
RETURNS TABLE(
    username VARCHAR,
    total_orders BIGINT,
    total_spent DECIMAL,
    last_order_date TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        u.username,
        COUNT(o.id)::BIGINT as total_orders,
        COALESCE(SUM(o.total_price), 0) as total_spent,
        MAX(o.order_date) as last_order_date
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.id = user_id_param
    GROUP BY u.id, u.username;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA saiql_test TO saiql_test_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA saiql_test TO saiql_test_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA performance_test TO saiql_test_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA performance_test TO saiql_test_user;
GRANT EXECUTE ON FUNCTION get_user_order_summary(INTEGER) TO saiql_test_user;
-- IHNpZ25hdHVyZTplYzUyNTBkZWRmNTZjZTZiMDA4NCBpZDpTQUlRTC1DSEFSTElFLVBST0QgYnk6QXBvbGxvICYgQ2xhdWRlIA==
