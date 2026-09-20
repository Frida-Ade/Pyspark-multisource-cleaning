from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


def create_spark_session():
    """
    Création de la session Spark.
    """

    spark = (
        SparkSession.builder
        .appName("MultiSourceDataCleaning")
        .master("local[*]")
        .getOrCreate()
    )

    return spark


def load_relational_data(spark):
    """
    Chargement des tables PostgreSQL via JDBC.
    """

    jdbc_url = "jdbc:postgresql://postgres_tp:5432/ecommerce"

    jdbc_properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }

    customers_df = spark.read.jdbc(
        url=jdbc_url,
        table="customers",
        properties=jdbc_properties
    )

    orders_df = spark.read.jdbc(
        url=jdbc_url,
        table="orders",
        properties=jdbc_properties
    )

    order_items_df = spark.read.jdbc(
        url=jdbc_url,
        table="order_items",
        properties=jdbc_properties
    )

    products_df = spark.read.jdbc(
        url=jdbc_url,
        table="products",
        properties=jdbc_properties
    )

    return (
        customers_df,
        orders_df,
        order_items_df,
        products_df
    )


def load_reviews_data(spark):
    """
    Chargement de la collection reviews depuis MongoDB.
    """

    reviews_df = (
        spark.read
        .format("mongodb")
        .option(
            "spark.mongodb.read.connection.uri",
            "mongodb://mongo_tp:27017/ecommerce.reviews"
        )
        .load()
    )

    return reviews_df


def load_delivery_events(spark):
    """
    Chargement des événements de livraison depuis le fichier JSON.
    """

    delivery_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField(
            "location",
            StructType([
                StructField("city", StringType(), True),
                StructField("country", StringType(), True)
            ]),
            True
        ),
        StructField(
            "carrier",
            StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True)
            ]),
            True
        )
    ])

    delivery_events_df = (
        spark.read
        .schema(delivery_schema)
        .option("multiLine", True)
        .json("/data/delivery_events.json")
    )

    return delivery_events_df