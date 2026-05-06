CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT,
    email VARCHAR(100),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    pet_type VARCHAR(20),
    pet_name VARCHAR(50),
    pet_breed VARCHAR(50)
);

CREATE TABLE dim_seller (
    seller_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    country VARCHAR(50),
    postal_code VARCHAR(20)
);

CREATE TABLE dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    contact_person VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(50)
);

CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    store_city VARCHAR(100),
    store_state VARCHAR(50),
    store_country VARCHAR(50),
    UNIQUE(store_city, store_state, store_country)
);

CREATE TABLE dim_store (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(100),
    store_address VARCHAR(255),
    store_phone VARCHAR(20),
    store_email VARCHAR(100),
    location_id INT,
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    UNIQUE(store_name, store_address)
);

CREATE TABLE dim_brand (
    brand_id SERIAL PRIMARY KEY,
    brand_name VARCHAR(100) UNIQUE
);

CREATE TABLE dim_product_category (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(50) UNIQUE
);

CREATE TABLE dim_product (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    product_price DECIMAL(10,2),
    product_weight DECIMAL(10,2),
    product_color VARCHAR(30),
    product_size VARCHAR(20),
    product_material VARCHAR(50),
    product_description TEXT,
    product_rating DECIMAL(3,2),
    product_reviews INT,
    product_release_date DATE,
    product_expiry_date DATE,
    pet_category VARCHAR(30),
    brand_id INT,
    category_id INT,
    FOREIGN KEY (brand_id) REFERENCES dim_brand(brand_id),
    FOREIGN KEY (category_id) REFERENCES dim_product_category(category_id),
    UNIQUE(product_name, product_price, product_color, product_size)
);

CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE,
    quantity INT,
    total_price DECIMAL(10,2),
    customer_id INT,
    seller_id INT,
    product_id INT,
    store_id INT,
    supplier_id INT,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (seller_id) REFERENCES dim_seller(seller_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
    FOREIGN KEY (supplier_id) REFERENCES dim_supplier(supplier_id)
);