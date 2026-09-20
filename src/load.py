from pyspark.sql.functions import *


def save_outputs(
    customer_order_360,
    validation_results,
    table_rejets
):

    # Étape 37 - Sauvegarde

    customer_order_360.write \
        .mode("overwrite") \
        .parquet(
            "output/customer_order_360")

    validation_results.write \
    .mode("overwrite") \
    .parquet(
        "output/data_quality_report")

    table_rejets.write \
    .mode("overwrite") \
    .parquet(
        "output/rejects")

    customer_order_360_partitionne = (
        customer_order_360
        .withColumn(
            "order_year",
            year(col("order_date_clean")))
        .withColumn(
            "order_month",
            month(col("order_date_clean"))))

    customer_order_360_partitionne = (
        customer_order_360_partitionne
        .coalesce(1))

    customer_order_360_partitionne.write \
        .mode("overwrite") \
        .partitionBy(
            "order_year",
            "order_month") \
        .parquet(
            "output/customer_order_360")


def validate_saved_outputs(spark, customer_order_360):

    # Étape 38 - Relecture Parquet

    customer_order_360_parquet = spark.read.parquet(
        "output/customer_order_360")

    validation_results_parquet = spark.read.parquet(
    "output/data_quality_report")

    table_rejets_parquet = spark.read.parquet(
    "output/rejects")

    print(
        "Customer Order 360 :",
        customer_order_360_parquet.count())

    print(
        "Validation Results :",
        validation_results_parquet.count())

    print(
        "Table Rejets :",
        table_rejets_parquet.count())

    print(
        "Volume original :",
        customer_order_360.count())

    print(
        "Volume relu :",
        customer_order_360_parquet.count())

    return (
        customer_order_360_parquet,
        validation_results_parquet,
        table_rejets_parquet)