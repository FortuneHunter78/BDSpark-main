from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, to_date, month, year, expr
from pyspark.sql.window import Window

def get_spark_session():
    return SparkSession.builder \
        .appName("Postgres-2-StarSchema") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
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

def write_to_postgres(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/bdspark") \
        .option("dbtable", table_name) \
        .option("user", "admin") \
        .option("password", "adminpassword") \
        .option("driver", "org.postgresql.Driver") \
        .option("truncate", "true") \
        .option("cascadeTruncate", "true") \
        .mode("overwrite") \
        .save()

def create_star_schema():
    spark = get_spark_session()

    raw_df = read_from_postgres(spark, "mock_data_raw")

    raw_clean_df = raw_df \
        .withColumn("sale_date_clean", to_date(col("sale_date"), "M/d/yyyy")) \
        .withColumn("product_release_date_clean", to_date(col("product_release_date"), "M/d/yyyy")) \
        .withColumn("product_expiry_date_clean", to_date(col("product_expiry_date"), "M/d/yyyy")) \
        .withColumn("product_price_clean", col("product_price").cast("double")) \
        .withColumn("product_weight_clean", col("product_weight").cast("double")) \
        .withColumn("sale_quantity_clean", col("sale_quantity").cast("int")) \
        .withColumn("product_rating_clean", col("product_rating").cast("double")) \
        .withColumn("product_reviews_clean", col("product_reviews").cast("int")) \
        .withColumn("customer_age_clean", col("customer_age").cast("int"))

    dim_customer_cols = ["customer_first_name", "customer_last_name", "customer_age_clean", "customer_email", "customer_country", "customer_postal_code", "customer_pet_type", "customer_pet_name", "customer_pet_breed"]
    dim_customer = raw_clean_df.select(dim_customer_cols).dropDuplicates() \
        .withColumnRenamed("customer_first_name", "first_name") \
        .withColumnRenamed("customer_last_name", "last_name") \
        .withColumnRenamed("customer_age_clean", "age") \
        .withColumnRenamed("customer_email", "email") \
        .withColumnRenamed("customer_country", "country") \
        .withColumnRenamed("customer_postal_code", "postal_code") \
        .withColumnRenamed("customer_pet_type", "pet_type") \
        .withColumnRenamed("customer_pet_name", "pet_name") \
        .withColumnRenamed("customer_pet_breed", "pet_breed") \
        .withColumn("customer_id", monotonically_increasing_id())

    dim_seller = raw_clean_df.select("seller_first_name", "seller_last_name", "seller_email", "seller_country", "seller_postal_code") \
        .dropDuplicates(["seller_email"]) \
        .withColumnRenamed("seller_first_name", "first_name") \
        .withColumnRenamed("seller_last_name", "last_name") \
        .withColumnRenamed("seller_email", "email") \
        .withColumnRenamed("seller_country", "country") \
        .withColumnRenamed("seller_postal_code", "postal_code") \
        .withColumn("seller_id", monotonically_increasing_id())

    dim_supplier = raw_clean_df.select("supplier_name", "supplier_contact", "supplier_email", "supplier_phone", "supplier_address", "supplier_city", "supplier_country") \
        .dropDuplicates(["supplier_name"]) \
        .withColumnRenamed("supplier_name", "name") \
        .withColumnRenamed("supplier_contact", "contact_person") \
        .withColumnRenamed("supplier_email", "email") \
        .withColumnRenamed("supplier_phone", "phone") \
        .withColumnRenamed("supplier_address", "address") \
        .withColumnRenamed("supplier_city", "city") \
        .withColumnRenamed("supplier_country", "country") \
        .withColumn("supplier_id", monotonically_increasing_id())

    dim_location = raw_clean_df.select("store_city", "store_state", "store_country").filter(col("store_city").isNotNull()) \
        .dropDuplicates() \
        .withColumn("location_id", monotonically_increasing_id())

    dim_store = raw_clean_df.select("store_name", "store_location", "store_phone", "store_email", "store_city", "store_state", "store_country").filter(col("store_name").isNotNull()) \
        .dropDuplicates(["store_name", "store_location"]) \
        .join(dim_location, on=["store_city", "store_state", "store_country"], how="left") \
        .select("store_name", "store_location", "store_phone", "store_email", "location_id") \
        .withColumnRenamed("store_location", "store_address") \
        .withColumn("store_id", monotonically_increasing_id())

    dim_brand = raw_clean_df.select("product_brand").filter(col("product_brand").isNotNull()).dropDuplicates() \
        .withColumnRenamed("product_brand", "brand_name") \
        .withColumn("brand_id", monotonically_increasing_id())

    dim_product_category = raw_clean_df.select("product_category").filter(col("product_category").isNotNull()).dropDuplicates() \
        .withColumnRenamed("product_category", "category_name") \
        .withColumn("category_id", monotonically_increasing_id())

    dim_product_cols = ["product_name", "product_price_clean", "product_weight_clean", "product_color", "product_size", "product_material", "product_description", "product_rating_clean", "product_reviews_clean", "product_release_date_clean", "product_expiry_date_clean", "pet_category", "product_brand", "product_category"]
    dim_product = raw_clean_df.select(dim_product_cols).filter(col("product_name").isNotNull()) \
        .dropDuplicates(["product_name", "product_price_clean", "product_color", "product_size"]) \
        .join(dim_brand, col("product_brand") == col("brand_name"), "left") \
        .join(dim_product_category, col("product_category") == col("category_name"), "left") \
        .select(
            "product_name", 
            col("product_price_clean").alias("product_price"),
            col("product_weight_clean").alias("product_weight"),
            "product_color", "product_size", "product_material", "product_description",
            col("product_rating_clean").alias("product_rating"),
            col("product_reviews_clean").alias("product_reviews"),
            col("product_release_date_clean").alias("product_release_date"),
            col("product_expiry_date_clean").alias("product_expiry_date"),
            "pet_category", "brand_id", "category_id"
        ).withColumn("product_id", monotonically_increasing_id())

    dim_date = raw_clean_df.select(col("sale_date_clean").alias("date")).dropDuplicates().filter(col("date").isNotNull()) \
        .withColumn("month", month(col("date"))) \
        .withColumn("year", year(col("date")))

    fact_sales = raw_clean_df.join(
        dim_customer,
        (raw_clean_df.customer_first_name.eqNullSafe(dim_customer.first_name)) & 
        (raw_clean_df.customer_last_name.eqNullSafe(dim_customer.last_name)) & 
        (raw_clean_df.customer_email.eqNullSafe(dim_customer.email)) & 
        (raw_clean_df.customer_age_clean.eqNullSafe(dim_customer.age)),
        "left"
    )

    fact_sales = fact_sales.join(dim_seller, (fact_sales.seller_email.eqNullSafe(dim_seller.email)), "left")

    fact_sales = fact_sales.join(
        dim_product,
        (fact_sales.product_name.eqNullSafe(dim_product.product_name)) & 
        (fact_sales.product_price_clean.eqNullSafe(dim_product.product_price)) & 
        (fact_sales.product_color.eqNullSafe(dim_product.product_color)) & 
        (fact_sales.product_size.eqNullSafe(dim_product.product_size)),
        "left"
    )

    fact_sales = fact_sales.join(dim_store, (fact_sales.store_name.eqNullSafe(dim_store.store_name)) & (fact_sales.store_location.eqNullSafe(dim_store.store_address)), "left")
    fact_sales = fact_sales.join(dim_supplier, (fact_sales.supplier_name.eqNullSafe(dim_supplier.name)), "left")
    
    fact_sales = fact_sales.select(
        col("sale_date_clean").alias("sale_date"),
        col("sale_quantity_clean").alias("quantity"),
        (col("sale_quantity_clean") * col("product_price_clean")).alias("total_price"),
        col("customer_id"),
        col("seller_id"),
        col("product_id"),
        col("store_id"),
        col("supplier_id")
    ).withColumn("sale_id", monotonically_increasing_id())

    write_to_postgres(dim_customer, "dim_customer")
    write_to_postgres(dim_seller, "dim_seller")
    write_to_postgres(dim_supplier, "dim_supplier")
    write_to_postgres(dim_location, "dim_location")
    write_to_postgres(dim_store, "dim_store")
    write_to_postgres(dim_brand, "dim_brand")
    write_to_postgres(dim_product_category, "dim_product_category")
    write_to_postgres(dim_product, "dim_product")
    write_to_postgres(dim_date, "dim_date")
    write_to_postgres(fact_sales, "fact_sales")
    
    print("Star schema created successfully in PostgreSQL.")
    spark.stop()

if __name__ == "__main__":
    create_star_schema()
