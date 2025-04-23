CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL
);

CREATE TABLE order_items (
    order_id INTEGER REFERENCES orders(order_id),
    item_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (order_id, item_id)
);

CREATE TABLE menu_items (
    item_id INTEGER PRIMARY KEY,
    item_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    description TEXT
);

COPY orders FROM '/raw_data/orders.csv' WITH (FORMAT csv, HEADER true);
COPY order_items FROM '/raw_data/order_item.csv' WITH (FORMAT csv, HEADER true);
COPY menu_items FROM '/raw_data/menu_items.csv' WITH (FORMAT csv, HEADER true);

-- Create indexes for better query performance
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_order_items_item_id ON order_items(item_id);
CREATE INDEX idx_menu_items_category ON menu_items(category);