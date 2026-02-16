CREATE TABLE IF NOT EXISTS products (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description VARCHAR(1000) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    status VARCHAR(50) NOT NULL,
    availability VARCHAR(50) NOT NULL,
    image_url VARCHAR(500),
    quantity INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_status ON products(status);