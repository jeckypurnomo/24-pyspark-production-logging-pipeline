from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum
import logging

# Create Logging Config
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

spark = None

try:

    # Create Spark Session
    spark = (
        SparkSession.builder
        .appName("PySparkProductionLoggingPipeline")
        .master("local[*]")
        .getOrCreate()
    )

    # Create Logger Object 
    logger = logging.getLogger("pipeline")

    logger.info("Pipeline Started")


    # ==== EXTRACT ====


    logger.info("Starting Extract Stage")

    # Read the Dataset
    orders_df = (
        spark.read.csv(
        "data/orders.csv",
        header=True,
        inferSchema=True
        )
    )

    # Count Records
    orders_count = orders_df.count()

    logger.info("Orders Extracted : %s", orders_count)

    # Display the Dataset
    print("\n--- Orders Dataset ---")
    orders_df.show()


    # ==== TRANSFORM ====


    logger.info("Starting Transform Stage")

    # Convert Date Type
    orders_date_df = (
        orders_df
        .withColumn(
            "order_date",
            to_date("order_date")
        )
    )

    # Display Orders Dataset After Date Converted
    print("\n--- Orders Schema After Date Converted ---")
    orders_date_df.printSchema()

    # Calculate Total Amount
    order_total_amount_df = (
        orders_date_df
        .withColumn(
            "total_amount",
            col("quantity") * col("unit_price")
        )
    )

    # Display Calculated Total Amount
    print("\n--- Orders with Total Amount ---")
    order_total_amount_df.show()

    # Validate the Dataset
    clean_orders_df = (
        order_total_amount_df
        .filter(
            (col("quantity") > 0) &
            (col("unit_price") > 0) &
            (col("order_date").isNotNull())
        )
    )

    # Display Validated Dataset
    print("\n--- Clean Orders Datastet ---")
    clean_orders_df.show()

    # Create Customer Sales
    customer_sales_df = (
        clean_orders_df
        .groupBy(
            "customer_id"
        )
        .agg(
            sum("total_amount").alias("total_sales")
        )
        .orderBy(
            col("total_sales")
            .desc()
        )
    )

    # Display Customer Sales
    print("\n--- Customer Sales ---")
    customer_sales_df.show()

    before_count = order_total_amount_df.count()
    after_count = clean_orders_df.count()
    invalid_count = before_count - after_count
    customer_sales_count = customer_sales_df.count()

    logger.info("Records Before Validation : %s", before_count)

    if invalid_count > 0:
        logger.warning("Invalid Records Detected : %s", invalid_count)

    else:
        logger.info("There is no Invalid Records")

    logger.info("Valid Records After Validation : %s", after_count)

    logger.info("Customer Sales Records : %s", customer_sales_count)

    logger.info("Transform Stage Completed")


    # ==== LOAD ====


    logger.info("Starting Load Stage")

    # Load the Dataset
    customer_sales_df.write \
        .mode("overwrite") \
        .parquet("output/customer_sales/")

    print("\nCustomer Sales Saved Successfully.")

    logger.info("Customer Sales Saved Successfully")

    logger.info("Load Stage Completed")

    logger.info("Pipeline Completed Successfully")

except Exception as e:
    logger.exception("Pipeline failed due to an unexpected error: %s", e)

finally:
    if spark is not None:
        spark.stop()
        logger.info("Spark Session Stopped")