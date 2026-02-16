CREATE TABLE IF NOT EXISTS warehouse_products (
    product_id UUID PRIMARY KEY,
    quantity INTEGER NOT NULL DEFAULT 0,
    width DOUBLE PRECISION,
    height DOUBLE PRECISION,
    depth DOUBLE PRECISION,
    weight DOUBLE PRECISION,
    fragile BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_warehouse_quantity ON warehouse_products(quantity);