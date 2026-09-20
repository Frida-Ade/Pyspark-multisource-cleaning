from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, trim, upper, regexp_replace, regexp_extract,
    lpad, concat, lit, when, current_timestamp,
    struct, to_json)
from pyspark.sql.functions import initcap, concat_ws
from pyspark.sql.functions import lower, count
from pyspark.sql.functions import substring
from pyspark.sql.functions import lower, regexp_replace, trim
from pyspark.sql.functions import to_date, current_date, months_between, floor, coalesce
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.functions import to_timestamp



def clean_customers(customers_df):

        # Étape 6 - Normalisation des identifiants clients
    
        customers_id_cleaning = (
        customers_df
        .withColumn("customer_id_raw", col("customer_id"))
        
        # 1. Suppression des espaces début/fin + majuscules
        .withColumn("customer_id_step1", upper(trim(col("customer_id"))))
        
        # 2. Suppression des tirets, underscores et espaces internes
        .withColumn("customer_id_step2", regexp_replace(col("customer_id_step1"), "[-_\\s]", ""))
        
        # 3. Remplacement du préfixe CUST par C
        .withColumn("customer_id_step3", regexp_replace(col("customer_id_step2"), "^CUST", "C"))
        
        # 4. Extraction de la partie numérique
        .withColumn("customer_id_number", regexp_extract(col("customer_id_step3"), "^C0*([0-9]+)$", 1))
        
        # 5. Création du format final C000001
        .withColumn(
            "customer_id_clean",
            when(
                col("customer_id_number") != "",
                concat(lit("C"), lpad(col("customer_id_number"), 6, "0"))
            ).otherwise(None)
            )
        )
            
        customers_rejects_step6 = (
            customers_id_cleaning
            .filter(col("customer_id_clean").isNull())
            .select(
                lit("customers").alias("source"),
                lit("Identifiant client impossible à normaliser").alias("rejection_reason"),
                current_timestamp().alias("rejection_timestamp"),
                to_json(struct([col(c) for c in customers_df.columns])).alias("original_data")))
        
        customers_step6 = (
        customers_id_cleaning
        .filter(col("customer_id_clean").isNotNull())
        .drop(
            "customer_id_raw",
            "customer_id_step1",
            "customer_id_step2",
            "customer_id_step3",
            "customer_id_number"))
    
    
        # Étape 7 - Nettoyage noms / prénoms
    
        customers_step7 = (
            customers_step6
            .withColumn("first_name_clean", trim(regexp_replace(col("first_name"), "\\s+", " ")))
            .withColumn("first_name_clean",when(col("first_name_clean") == "", None).otherwise(initcap(col("first_name_clean"))))
            .withColumn("last_name_clean",trim(regexp_replace(col("last_name"), "\\s+", " ")))
            .withColumn("last_name_clean",when(col("last_name_clean") == "", None).otherwise(initcap(col("last_name_clean"))))
            .withColumn("full_name",concat_ws(" ", col("first_name_clean"), col("last_name_clean")))
        )
                
        customers_step7.select(
            spark_sum(when(col("first_name_clean").isNull(), 1).otherwise(0)).alias("first_name_clean_nulls"),
            spark_sum(when(col("last_name_clean").isNull(), 1).otherwise(0)).alias("last_name_clean_nulls"),
            spark_sum(when(col("full_name").isNull(), 1).otherwise(0)).alias("full_name_nulls")
        ).show()
    
        # Étape 8 - Validation emails
    
        email_regex = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
    
        customers_step8 = (
            customers_step7
            .withColumn("email_tmp",lower(regexp_replace(trim(col("email")), "\\s+", "")))
            .withColumn("email_tmp",when(col("email_tmp") == "", None).otherwise(col("email_tmp")))
            .withColumn("is_email_valid",when(col("email_tmp").isNotNull() & col("email_tmp").rlike(email_regex),True).otherwise(False))
            .withColumn("email_clean",when(col("is_email_valid") == True, col("email_tmp")).otherwise(None)))
        
        emails_dupliques = (
            customers_step8
            .filter(col("email_clean").isNotNull())
            .groupBy("email_clean")
            .agg(count("*").alias("nombre_occurrences"))
            .filter(col("nombre_occurrences") > 1))
        
        emails_dupliques.show(truncate=False)
        
        customers_step8 = (
            customers_step8
            .join(
                emails_dupliques
                .select("email_clean")
                .withColumn("is_email_duplicate", lit(True)),
                on="email_clean",
                how="left")
            .withColumn(
                "is_email_duplicate",
                when(col("is_email_duplicate").isNull(), False)
                .otherwise(col("is_email_duplicate"))))
    
        # Étape 9 - Téléphones
    
        customers_step9 = (
            customers_step8
            .withColumn("phone_digits",regexp_replace(col("phone"), "[^0-9]", ""))
            .withColumn("phone_clean",when(
                    col("phone_digits").rlike("^0[1-9][0-9]{8}$"),
                    concat(lit("+33"), substring(col("phone_digits"), 2, 9)))
                .when(
                    col("phone_digits").rlike("^33[1-9][0-9]{8}$"),
                    concat(lit("+"), col("phone_digits")))
                .when(
                    col("phone_digits").rlike("^0033[1-9][0-9]{8}$"),
                    concat(lit("+33"), substring(col("phone_digits"), 5, 9)))
                .otherwise(None))
            .withColumn("is_phone_valid",when(col("phone_clean").isNotNull(), True).otherwise(False)))
               
    
        customers_step9.filter(
            col("phone_clean").isNull()
        ).select("customer_id_clean","full_name","phone","phone_digits","phone_clean","is_phone_valid").show(truncate=False)
        
    
        # Étape 10 - Villes et pays
    
        customers_step10_tmp = (
            customers_step9
            .withColumn("city_tmp",lower(trim(regexp_replace(col("city"), "\\s+", " "))))
            .withColumn("city_tmp",regexp_replace(col("city_tmp"), "-", " "))
            .withColumn("city_tmp",regexp_replace(col("city_tmp"), "\\s+", " "))
            .withColumn("city_tmp",trim(col("city_tmp")))
            .withColumn("country_tmp",lower(trim(regexp_replace(col("country"), "\\s+", " ")))))
        
        customers_step10_tmp = (
            customers_step10_tmp
            .withColumn(
                "city_clean",
                when(col("city_tmp").rlike("^paris.*"), "Paris")
                .when(col("city_tmp").isin("lyon", "lyon 69000"), "Lyon")
                .when(col("city_tmp").isin("marseille", "marseille 13000"), "Marseille")
                .when(col("city_tmp").isin("toulouse"), "Toulouse")
                .when(col("city_tmp").isin("bordeaux"), "Bordeaux")
                .when(col("city_tmp").isin("lille"), "Lille")
                .when(col("city_tmp").isin("nantes"), "Nantes")
                .when(col("city_tmp").isin("nice"), "Nice")
                .otherwise(initcap(col("city_tmp")))))
        
        customers_step10 = (
            customers_step10_tmp
            .withColumn(
                "country_clean",
                when(col("country_tmp").isin("fr", "france", "french republic"), "France")
                .when(col("country_tmp").isin("ma", "maroc", "morocco"), "Maroc")
                .when(col("country_tmp").isin("us", "usa", "united states"), "United States")
                .when(col("country_tmp").isin("uk", "gb", "united kingdom"), "United Kingdom")
                .otherwise(initcap(col("country_tmp")))))
        
    
        customers_step10.select(
            spark_sum(when(col("city_clean").isNull(), 1).otherwise(0)).alias("city_clean_nulls"),
            spark_sum(when(col("country_clean").isNull(), 1).otherwise(0)).alias("country_clean_nulls")
        ).show()
    
        # Étape 11 - Dates de naissance
    
         customers_step11_tmp = (
            customers_step10
            .withColumn(
                "birth_date_clean",
                coalesce(
                    to_date(col("birth_date"), "yyyy-MM-dd"),
                    to_date(col("birth_date"), "dd/MM/yyyy"),
                    to_date(col("birth_date"), "yyyy/MM/dd"),
                    to_date(col("birth_date"), "dd-MM-yyyy")))
            .withColumn("age",floor(months_between(current_date(), col("birth_date_clean")) / 12)))
        
        customers_step11_tmp = (
            customers_step11_tmp
            .withColumn(
                "is_birth_date_valid",
                when(
                    col("birth_date_clean").isNull()
                    | (col("birth_date_clean") > current_date())
                    | (col("age") < 0)
                    | (col("age") > 120),
                    False
                ).otherwise(True)))
    
        customers_rejects_step11 = (
            customers_step11_tmp
            .filter(col("is_birth_date_valid") == False)
            .select(
                lit("customers").alias("source"),
                lit("Date de naissance invalide").alias("rejection_reason"),
                current_timestamp().alias("rejection_timestamp"),
                to_json(struct([col(c) for c in customers_step10.columns])).alias("original_data")))
            
        customers_step11 = (
            customers_step11_tmp
            .filter(col("is_birth_date_valid") == True))
        

    # Étape 12 - Déduplication

        window_customers = Window.partitionBy(
            "customer_id_clean"
        ).orderBy(
            col("is_email_valid").desc(),
            col("created_at").desc())
    
        customers_step12_tmp = (
            customers_step11_tmp
            .withColumn(
                "row_num",
                row_number().over(window_customers)))
    
        customers_clean = (
            customers_step12_tmp
            .filter(col("row_num") == 1)
            .drop("row_num"))
    
        return (
            customers_clean,
            customers_rejects_step6,
            customers_rejects_step11)

from pyspark.sql.functions import *
from pyspark.sql.functions import broadcast


def clean_orders(orders_df, spark):

        # Étape 13 - Normalisation commandes
    
        orders_step13_tmp = (
            orders_df
            .withColumn("order_id_tmp", upper(trim(col("order_id"))))
            .withColumn(
                "order_id_tmp",
                regexp_replace(col("order_id_tmp"), "[-_\\s]", "")
            )
            .withColumn(
                "order_id_number",
                regexp_extract(
                    col("order_id_tmp"),
                    "^ORD0*([0-9]+)$",
                    1
                )
            )
            .withColumn(
                "order_id_clean",
                when(
                    col("order_id_number") != "",
                    concat(
                        lit("ORD"),
                        lpad(col("order_id_number"), 6, "0")
                    )
                ).otherwise(None)
            )
        )
    
        orders_step13_tmp = (
            orders_step13_tmp
            .withColumn(
                "customer_id_step1",
                upper(trim(col("customer_id")))
            )
            .withColumn(
                "customer_id_step2",
                regexp_replace(
                    col("customer_id_step1"),
                    "[-_\\s]",
                    ""
                )
            )
            .withColumn(
                "customer_id_step3",
                regexp_replace(
                    col("customer_id_step2"),
                    "^CUST",
                    "C"
                )
            )
            .withColumn(
                "customer_id_number",
                regexp_extract(
                    col("customer_id_step3"),
                    "^C0*([0-9]+)$",
                    1
                )
            )
            .withColumn(
                "customer_id_clean",
                when(
                    col("customer_id_number") != "",
                    concat(
                        lit("C"),
                        lpad(col("customer_id_number"), 6, "0")
                    )
                ).otherwise(None)
            )
        )
    
        orders_step13_tmp = (
            orders_step13_tmp
            .withColumn(
                "order_date_clean",
                coalesce(
                    to_timestamp(
                        col("order_date"),
                        "yyyy-MM-dd HH:mm:ss"
                    ),
                    to_timestamp(
                        col("order_date"),
                        "yyyy-MM-dd"
                    ),
                    to_timestamp(
                        col("order_date"),
                        "dd/MM/yyyy"
                    ),
                    to_timestamp(
                        col("order_date"),
                        "yyyy/MM/dd"
                    ),
                    to_timestamp(
                        col("order_date"),
                        "dd-MM-yyyy"
                    )
                )
            )
        )
    
        orders_rejects_step13 = (
            orders_step13_tmp
            .filter(col("order_date_clean").isNull())
            .select(
                lit("orders").alias("source"),
                lit("Date de commande invalide")
                .alias("rejection_reason"),
                current_timestamp()
                .alias("rejection_timestamp"),
                to_json(
                    struct(
                        [col(c) for c in orders_df.columns]
                    )
                ).alias("original_data")
            )
        )
    
        orders_clean_step13 = (
            orders_step13_tmp
            .filter(
                col("order_date_clean").isNotNull()
            )
        )
    
        # Étape 14 - Normalisation statuts
    
        orders_step14 = (
            orders_clean_step13
            .withColumn(
                "status_tmp",
                upper(trim(col("status")))
            )
            .withColumn(
                "order_status",
                when(
                    col("status_tmp")
                    .isin("PAID", "PAYÉE"),
                    "PAID"
                )
                .when(
                    col("status_tmp")
                    .isin("COMPLETED"),
                    "DELIVERED"
                )
                .when(
                    col("status_tmp")
                    .isin("CANCELLED", "ANNULÉE"),
                    "CANCELLED"
                )
                .when(
                    col("status_tmp")
                    .isin("CREATED"),
                    "CREATED"
                )
                .when(
                    col("status_tmp")
                    .isin("PREPARING"),
                    "PREPARING"
                )
                .when(
                    col("status_tmp")
                    .isin("SHIPPED"),
                    "SHIPPED"
                )
                .when(
                    col("status_tmp")
                    .isin("RETURNED"),
                    "RETURNED"
                )
                .otherwise("UNKNOWN")
            )
        )
    
        # Étape 15 - Devises et conversion EUR
    
        allowed_currencies = [
            "EUR",
            "USD",
            "GBP",
            "MAD"
        ]
    
        exchange_rates = spark.createDataFrame(
            [
                ("EUR", 1.00),
                ("USD", 0.92),
                ("GBP", 1.17),
                ("MAD", 0.092)
            ],
            [
                "currency",
                "rate_to_eur"
            ]
        )
    
        orders_step15 = (
            orders_step14
            .join(
                broadcast(exchange_rates),
                on="currency",
                how="left"
            )
        )
    
        orders_step15 = (
            orders_step15
            .withColumn(
                "total_amount_eur",
                round(
                    col("total_amount")
                    * col("rate_to_eur"),
                    2
                )
            )
        )
    
        orders_rejects_step15 = (
            orders_step15
            .filter(
                (col("total_amount") <= 0)
                |
                col("currency").isNull()
                |
                (~col("currency")
                 .isin(allowed_currencies))
            )
        )
    
        orders_clean = (
            orders_step15
            .filter(col("total_amount") > 0)
            .filter(
                col("currency")
                .isin(allowed_currencies)
            )
        )
    
        return (
            orders_clean,
            orders_rejects_step13,
            orders_rejects_step15,
            exchange_rates
        )


from pyspark.sql.functions import *
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import broadcast


def clean_order_items(order_items_df, orders_clean):

        # Étape 16 - Validation des lignes de commande
    
        order_items_step16_tmp = (
            order_items_df
            .withColumn(
                "discount",
                when(col("discount").isNull(), 0)
                .otherwise(col("discount"))
            )
        )
    
        order_items_step16_tmp = (
            order_items_step16_tmp
            .withColumn(
                "is_valid",
                when(
                    (col("quantity") <= 0)
                    |
                    (col("unit_price") < 0)
                    |
                    (col("discount") < 0)
                    |
                    (col("discount") > 1),
                    False
                ).otherwise(True)
            )
        )
    
        order_items_rejects_step16 = (
            order_items_step16_tmp
            .filter(col("is_valid") == False)
            .select(
                lit("order_items").alias("source"),
                lit(
                    "Quantité, prix ou remise invalide"
                ).alias("rejection_reason"),
                current_timestamp()
                .alias("rejection_timestamp"),
                to_json(
                    struct(
                        [col(c) for c in order_items_df.columns]
                    )
                ).alias("original_data")
            )
        )
    
        order_items_step16 = (
            order_items_step16_tmp
            .filter(col("is_valid") == True)
        )
    
        order_items_step16 = (
            order_items_step16
            .withColumn(
                "gross_amount",
                col("quantity") * col("unit_price")
            )
            .withColumn(
                "discount_amount",
                col("gross_amount") * col("discount")
            )
            .withColumn(
                "net_amount",
                col("gross_amount")
                - col("discount_amount")
            )
        )
    
        order_items_step16 = (
            order_items_step16
            .withColumn(
                "order_id_tmp",
                upper(trim(col("order_id")))
            )
            .withColumn(
                "order_id_tmp",
                regexp_replace(
                    col("order_id_tmp"),
                    "[-_\\s]",
                    ""
                )
            )
            .withColumn(
                "order_id_number",
                regexp_extract(
                    col("order_id_tmp"),
                    "^ORD0*([0-9]+)$",
                    1
                )
            )
            .withColumn(
                "order_id_clean",
                when(
                    col("order_id_number") != "",
                    concat(
                        lit("ORD"),
                        lpad(
                            col("order_id_number"),
                            6,
                            "0"
                        )
                    )
                ).otherwise(None)
            )
        )
    
        order_items_summary = (
            order_items_step16
            .groupBy("order_id_clean")
            .agg(
                countDistinct("product_id")
                .alias("number_of_products"),
    
                spark_sum("quantity")
                .alias("total_quantity"),
    
                spark_sum("gross_amount")
                .alias("gross_amount_total"),
    
                spark_sum("discount_amount")
                .alias("discount_total"),
    
                spark_sum("net_amount")
                .alias("net_amount_total")
            )
        )
    
        # Étape 17 - Comparaison des montants
    
        amount_comparison = (
            orders_clean
            .join(
                order_items_summary,
                on="order_id_clean",
                how="left"
            )
        )
    
        amount_comparison = (
            amount_comparison
            .withColumn(
                "amount_difference",
                abs(
                    col("total_amount_eur")
                    - col("net_amount_total")
                )
            )
            .withColumn(
                "is_amount_consistent",
                when(
                    col("amount_difference") <= 0.01,
                    True
                ).otherwise(False)
            )
        )
    
        return (
            order_items_step16,
            order_items_summary,
            amount_comparison,
            order_items_rejects_step16
        )

def clean_products(products_df):

        # Étape 18 - Nettoyage des produits
    
        # Normalisation identifiant produit
    
        products_step18_tmp = (
            products_df
            .withColumn(
                "product_id_tmp",
                upper(trim(col("product_id"))))
            .withColumn(
                "product_id_tmp",
                regexp_replace(
                    col("product_id_tmp"),
                    "[-_\\s]",
                    ""))
            .withColumn(
                "product_id_clean",
                col("product_id_tmp")))
    
        # Nettoyage nom produit
    
        products_step18_tmp = (
            products_step18_tmp
            .withColumn(
                "product_name_clean",
                when(
                    trim(col("product_name")) == "",
                    None
                ).otherwise(
                    initcap(
                        trim(
                            regexp_replace(
                                col("product_name"),
                                "\\s+",
                                " "))))))
    
        # Nettoyage marque
    
        products_step18_tmp = (
            products_step18_tmp
            .withColumn(
                "brand_clean",
                when(
                    trim(col("brand")) == "",
                    None
                ).otherwise(
                    initcap(
                        trim(
                            regexp_replace(
                                col("brand"),
                                "\\s+",
                                " "))))))
    
        # Normalisation catégories
    
        products_step18 = (
            products_step18_tmp
            .withColumn(
                "category_tmp",
                lower(
                    trim(
                        regexp_replace(
                            col("category"),
                            "\\s+",
                            " "))))
            .withColumn(
                "category_clean",
                when(
                    col("category_tmp").isin(
                        "high-tech",
                        "high tech",
                        "informatique",
                        "electronique",
                        "électronique"
                    ),
                    "Technologie"
                ).otherwise(
                    initcap(col("category_tmp")))))
    
        # Déduplication
    
        window_products = (
            Window.partitionBy(
                "product_id_clean"
            )
            .orderBy(
                col("active").desc()))
    
        products_step18 = (
            products_step18
            .withColumn(
                "row_num",
                row_number().over(window_products)))
    
        products_clean = (
            products_step18
            .filter(col("row_num") == 1)
            .drop("row_num")
        )
    
        return products_clean

from pyspark.sql.functions import *
from pyspark.sql.window import Window


def clean_reviews(reviews_df, orders_clean):

        # Étape 19 - Normalisation des identifiants
    
        reviews_step19 = (
            reviews_df
            .withColumn(
                "customer_id_tmp",
                upper(trim(col("customerId")))
            )
            .withColumn(
                "customer_id_tmp",
                regexp_replace(
                    col("customer_id_tmp"),
                    "[-_\\s]",
                    ""
                )
            )
            .withColumn(
                "customer_id_tmp",
                regexp_replace(
                    col("customer_id_tmp"),
                    "^CUST",
                    "C"
                )
            )
            .withColumn(
                "customer_id_number",
                regexp_extract(
                    col("customer_id_tmp"),
                    "^C0*([0-9]+)$",
                    1
                )
            )
            .withColumn(
                "customer_id_clean",
                when(
                    col("customer_id_number") != "",
                    concat(
                        lit("C"),
                        lpad(col("customer_id_number"), 6, "0")
                    )
                ).otherwise(None)
            )
        )
    
        reviews_step19 = (
            reviews_step19
            .withColumn(
                "product_id_clean",
                upper(
                    regexp_replace(
                        trim(col("productId")),
                        "[-_\\s]",
                        ""
                    )
                )
            )
        )
    
        reviews_step19 = (
            reviews_step19
            .withColumn(
                "order_id_tmp",
                upper(trim(col("orderId")))
            )
            .withColumn(
                "order_id_tmp",
                regexp_replace(
                    col("order_id_tmp"),
                    "[-_\\s]",
                    ""
                )
            )
            .withColumn(
                "order_id_number",
                regexp_extract(
                    col("order_id_tmp"),
                    "^ORD0*([0-9]+)$",
                    1
                )
            )
            .withColumn(
                "order_id_clean",
                when(
                    col("order_id_number") != "",
                    concat(
                        lit("ORD"),
                        lpad(col("order_id_number"), 6, "0")
                    )
                ).otherwise(None)
            )
        )
    
        reviews_step19 = (
            reviews_step19
            .withColumn(
                "review_date",
                coalesce(
                    to_timestamp(col("reviewDate")),
                    to_timestamp(
                        col("reviewDate"),
                        "yyyy-MM-dd HH:mm:ss"
                    )
                )
            )
        )
    
        reviews_step19 = (
            reviews_step19
            .withColumn(
                "verified_purchase",
                when(
                    lower(
                        col("verifiedPurchase")
                        .cast("string")
                    ).isin("true", "1", "yes"),
                    True
                )
                .when(
                    lower(
                        col("verifiedPurchase")
                        .cast("string")
                    ).isin("false", "0", "no"),
                    False
                )
                .otherwise(None)
            )
        )
    
        # Étape 20 - Validation des notes
    
        reviews_step20 = (
            reviews_step19
            .withColumn(
                "is_rating_valid",
                when(
                    (col("rating") >= 1)
                    &
                    (col("rating") <= 5),
                    True
                ).otherwise(False)
            )
        )
    
        reviews_step20 = (
            reviews_step20
            .withColumn(
                "comment",
                when(
                    trim(col("comment")) == "",
                    None
                ).otherwise(col("comment"))
            )
        )
    
        reviews_rejects_step20 = (
            reviews_step20
            .filter(col("is_rating_valid") == False)
            .select(
                lit("reviews").alias("source"),
                lit(
                    "Note hors intervalle [1,5]"
                ).alias("rejection_reason"),
                current_timestamp()
                .alias("rejection_timestamp"),
                to_json(
                    struct(
                        [col(c) for c in reviews_df.columns]
                    )
                ).alias("original_data")
            )
        )
    
        reviews_valid = (
            reviews_step20
            .filter(col("is_rating_valid") == True)
        )
    
        # Étape 21 - Déduplication
    
        reviews_step21_tmp = (
            reviews_valid
            .withColumn(
                "comment_length",
                when(
                    col("comment").isNull(),
                    0
                ).otherwise(
                    length(col("comment"))
                )
            )
        )
    
        window_reviews = (
            Window.partitionBy(
                "customer_id_clean",
                "product_id_clean",
                "order_id_clean"
            )
            .orderBy(
                col("review_date").desc(),
                col("comment_length").desc()
            )
        )
    
        reviews_step21_tmp = (
            reviews_step21_tmp
            .withColumn(
                "row_num",
                row_number().over(window_reviews)
            )
        )
    
        reviews_clean = (
            reviews_step21_tmp
            .filter(col("row_num") == 1)
            .drop(
                "row_num",
                "comment_length"
            )
        )
    
        # Étape 22 - Vérification achat vérifié
    
        reviews_step22 = (
            reviews_clean.alias("r")
            .join(
                orders_clean.select(
                    "order_id_clean",
                    "customer_id_clean"
                ).alias("o"),
                on="order_id_clean",
                how="left"
            )
        )
    
        reviews_step22 = (
            reviews_step22
            .withColumn(
                "order_exists",
                col("o.order_id_clean").isNotNull()
            )
            .withColumn(
                "customer_order_match",
                col("r.customer_id_clean")
                ==
                col("o.customer_id_clean")
            )
        )
    
        reviews_step22 = (
            reviews_step22
            .withColumn(
                "verified_purchase_computed",
                col("order_exists")
                &
                col("customer_order_match")
            )
        )
    
        reviews_step22 = (
            reviews_step22
            .withColumn(
                "is_verification_consistent",
                col("verified_purchase")
                ==
                col("verified_purchase_computed")
            )
        )
    
        reviews_clean = reviews_step22
    
        return (
            reviews_clean,
            reviews_rejects_step20
        )

def clean_delivery_events(delivery_events_df, orders_clean):

        # Étape 23 - Aplatissement du JSON
    
        delivery_step23 = (
            delivery_events_df
            .select(
                "event_id",
                "order_id",
                "event_type",
                "event_timestamp",
                col("location.city").alias("delivery_city"),
                col("location.country").alias("delivery_country"),
                col("carrier.id").alias("carrier_id"),
                col("carrier.name").alias("carrier_name")
            )
        )
    
        # Étape 24 - Nettoyage et normalisation
    
        delivery_step24 = (
            delivery_step23
            .withColumn(
                "order_id_tmp",
                upper(trim(col("order_id")))
            )
            .withColumn(
                "order_id_tmp",
                regexp_replace(
                    col("order_id_tmp"),
                    "[-_\\s]",
                    ""
                )
            )
            .withColumn(
                "order_id_number",
                regexp_extract(
                    col("order_id_tmp"),
                    "^ORD0*([0-9]+)$",
                    1
                )
            )
            .withColumn(
                "order_id_clean",
                when(
                    col("order_id_number") != "",
                    concat(
                        lit("ORD"),
                        lpad(
                            col("order_id_number"),
                            6,
                            "0"
                        )
                    )
                )
            )
            .withColumn(
                "event_type_clean",
                upper(trim(col("event_type")))
            )
            .withColumn(
                "event_timestamp_clean",
                to_timestamp(col("event_timestamp"))
            )
            .withColumn(
                "carrier_name_clean",
                initcap(trim(col("carrier_name")))
            )
            .withColumn(
                "delivery_city",
                initcap(trim(col("delivery_city")))
            )
            .withColumn(
                "delivery_country",
                initcap(trim(col("delivery_country")))
            )
        )
    
        delivery_step24 = (
            delivery_step24
            .withColumn(
                "event_type_clean",
                when(
                    col("event_type_clean").isin(
                        "ORDER_CREATED",
                        "PREPARING",
                        "SHIPPED",
                        "IN_TRANSIT",
                        "DELIVERED",
                        "RETURNED"
                    ),
                    col("event_type_clean")
                )
                .otherwise("UNKNOWN")
            )
        )
    
        # Étape 25 - Suppression des doublons
    
        delivery_step25 = (
            delivery_step24
            .dropDuplicates(
                [
                    "order_id_clean",
                    "event_type_clean",
                    "event_timestamp_clean",
                    "carrier_id"
                ]
            )
        )
    
        # Étape 26 - Dernier événement
    
        window_delivery = (
            Window
            .partitionBy("order_id_clean")
            .orderBy(
                col("event_timestamp_clean").desc()
            )
        )
    
        delivery_ranked = (
            delivery_step25
            .withColumn(
                "row_num",
                row_number().over(window_delivery)
            )
        )
    
        last_delivery_event = (
            delivery_ranked
            .filter(col("row_num") == 1)
        )
    
        # Étape 27 - Résumé livraison
    
        delivery_summary = (
            delivery_step25
            .groupBy("order_id_clean")
            .agg(
                min("event_timestamp_clean")
                .alias("first_event"),
    
                max("event_timestamp_clean")
                .alias("last_event"),
    
                count("*")
                .alias("number_of_events")
            )
        )
    
        delivery_summary = (
            delivery_summary
            .join(
                last_delivery_event.select(
                    "order_id_clean",
                    "event_type_clean",
                    "carrier_name_clean",
                    "delivery_city"
                ),
                on="order_id_clean",
                how="left"
            )
        )
    
        delivery_summary = (
            delivery_summary
            .withColumnRenamed(
                "event_type_clean",
                "last_delivery_status"
            )
        )
    
        delivery_summary = (
            delivery_summary
            .join(
                orders_clean.select(
                    "order_id_clean",
                    "order_date_clean"
                ),
                on="order_id_clean",
                how="left"
            )
        )
    
        delivery_summary = (
            delivery_summary
            .withColumn(
                "delivery_delay_days",
                datediff(
                    col("last_event"),
                    col("order_date_clean")
                )
            )
        )
    
        delivery_summary = (
            delivery_summary
            .withColumn(
                "delivery_performance",
                when(
                    col("delivery_delay_days")
                    .between(0, 2),
                    "Très rapide"
                )
                .when(
                    col("delivery_delay_days")
                    .between(3, 5),
                    "Normal"
                )
                .when(
                    col("delivery_delay_days")
                    .between(6, 10),
                    "Lent"
                )
                .when(
                    col("delivery_delay_days") > 10,
                    "Très lent"
                )
                .otherwise("Non livré")
            )
        )
    
        return delivery_summary
        
