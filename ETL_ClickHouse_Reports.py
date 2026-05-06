from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

def get_spark_session():
    return SparkSession.builder \
        .appName("StarSchema-2-ClickHouse") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,com.clickhouse:clickhouse-jdbc:0.4.6") \
        .getOrCreate()

def read_from_postgres(spark, table_name):
    return spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/bdspark") \
        .option("dbtable", table_name) \
        .option("user", "admin") \
        .option("password", "adminpassword") \
        .option("driver", "org.postgresql.Driver") \
        .load()

def write_to_clickhouse(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:ch://clickhouse:8123/default?compress=0") \
        .option("dbtable", table_name) \
        .option("user", "default") \
        .option("password", "defaultpassword") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("batchsize", "1000") \
        .option("isolationLevel", "NONE") \
        .mode("overwrite") \
        .save()

def create_reports():
    spark = get_spark_session()

    dim_product = read_from_postgres(spark, "dim_product")
    dim_customer = read_from_postgres(spark, "dim_customer")
    dim_location = read_from_postgres(spark, "dim_location")
    dim_store = read_from_postgres(spark, "dim_store")
    dim_supplier = read_from_postgres(spark, "dim_supplier")
    dim_date = read_from_postgres(spark, "dim_date")
    fact_sales = read_from_postgres(spark, "fact_sales")

    dim_store_full = dim_store.join(dim_location, on="location_id", how="left")
    
    fact_sales_alias = fact_sales.alias("f")

    sales_with_products = fact_sales_alias.join(dim_product.alias("p"), col("f.product_id") == col("p.product_id"))
    sales_with_customers = fact_sales_alias.join(dim_customer.alias("c"), col("f.customer_id") == col("c.customer_id"))
    sales_with_date = fact_sales_alias.join(dim_date.alias("d"), col("f.sale_date") == col("d.date"))
    sales_with_stores = fact_sales_alias.join(dim_store_full.alias("st"), col("f.store_id") == col("st.store_id"))

    sales_with_suppliers_and_products = fact_sales_alias \
        .join(dim_supplier.alias("su"), col("f.supplier_id") == col("su.supplier_id")) \
        .join(dim_product.alias("p"), col("f.product_id") == col("p.product_id"))

    mart_product = sales_with_products.groupBy(
        "p.product_id", "p.product_name", "p.pet_category", "p.product_rating", "p.product_reviews"
    ).agg(
        sum("f.total_price").alias("total_revenue"),
        sum("f.quantity").alias("total_sold")
    ).fillna("Unknown")
    write_to_clickhouse(mart_product, "mart_product")

    mart_customer = sales_with_customers.groupBy(
        "c.customer_id", "c.first_name", "c.last_name", "c.country"
    ).agg(
        sum("f.total_price").alias("total_spent"),
        avg("f.total_price").alias("avg_check"),
        count("f.sale_id").alias("purchases_count")
    ).fillna("Unknown")
    write_to_clickhouse(mart_customer, "mart_customer")

    mart_date = sales_with_date.groupBy("d.year", "d.month").agg(
        sum("f.total_price").alias("total_revenue"),
        avg("f.total_price").alias("avg_order_size"),
        sum("f.quantity").alias("total_items_sold")
    )
    write_to_clickhouse(mart_date, "mart_date")

    mart_store = sales_with_stores.groupBy(
        "st.store_id", "st.store_name", "st.store_city", "st.store_country"
    ).agg(
        sum("f.total_price").alias("total_revenue"),
        avg("f.total_price").alias("avg_check"),
        sum("f.quantity").alias("total_items_sold")
    ).fillna("Unknown")
    write_to_clickhouse(mart_store, "mart_store")

    mart_supplier = sales_with_suppliers_and_products.groupBy(
        "su.supplier_id", "su.name", "su.country"
    ).agg(
        sum("f.total_price").alias("total_revenue"),
        avg("p.product_price").alias("avg_product_price"),
        sum("f.quantity").alias("total_items_sold")
    ).fillna("Unknown")
    write_to_clickhouse(mart_supplier, "mart_supplier")

    mart_quality = sales_with_products.groupBy(
        "p.product_id", "p.product_name", "p.product_rating", "p.product_reviews"
    ).agg(
        sum("f.quantity").alias("total_sold"),
        sum("f.total_price").alias("total_revenue")
    ).fillna("Unknown")
    write_to_clickhouse(mart_quality, "mart_quality")

    print("""All 6 Data Marts generated successfully.
    Querying in ClickHouse allows you to apply `ORDER BY`, `LIMIT X` 
    and aggregate metrics corresponding perfectly to the required points. """)
    spark.stop()

if __name__ == "__main__":
    create_reports()