from pyspark.sql.functions import *


def create_customer_order_360(
    orders_clean,
    customers_clean,
    order_items_step16,
    order_items_summary,
    products_clean,
    reviews_clean,
    delivery_summary
):

    # Étape 28 - Jointure clients / commandes

    jointure_clients_commandes = (
        orders_clean.alias("o")
        .join(
            customers_clean.alias("c"),
            on="customer_id_clean",
            how="left")
        .withColumn(
            "customer_found",
            col("c.customer_id_clean").isNotNull()))

    commandes_orphelines = (
        jointure_clients_commandes
        .filter(col("customer_found") == False))

    # Étape 29 - Jointure commandes / articles

    jointure_commandes_articles = (
        jointure_clients_commandes
        .join(
            order_items_summary,
            on="order_id_clean",
            how="left"))

    jointure_commandes_articles = (
        jointure_commandes_articles
        .withColumn(
            "amount_difference",
            abs(
                col("total_amount_eur")
                - col("net_amount_total"))))

    # Étape 30 - Jointure commandes / produits

    order_items_step16 = (
        order_items_step16
        .withColumn(
            "product_id_clean",
            upper(
                regexp_replace(
                    trim(col("product_id")),
                    "[-_\\s]",
                    ""))))

    details_produits_commande = (
        order_items_step16.alias("oi")
        .join(
            products_clean.alias("p"),
            on="product_id_clean",
            how="left")
        .groupBy("order_id_clean")
        .agg(
            collect_set("product_name_clean")
            .alias("liste_produits"),

            collect_set("category_clean")
            .alias("liste_categories"),

            collect_set("brand_clean")
            .alias("liste_marques"),

            countDistinct("category_clean")
            .alias("nombre_categories_differentes")))

    jointure_commandes_produits = (
        jointure_commandes_articles
        .join(
            details_produits_commande,
            on="order_id_clean",
            how="left"))

    # Étape 31 - Agrégation des avis

    resume_avis_commandes = (
        reviews_clean
        .groupBy("order_id_clean")
        .agg(
            count("*").alias("nombre_avis"),

            avg("rating")
            .alias("note_moyenne"),

            min("rating")
            .alias("note_min"),

            max("rating")
            .alias("note_max"),

            spark_sum(
                when(
                    col("verified_purchase") == True,
                    1
                ).otherwise(0)
            ).alias("nombre_achats_verifies"),

            spark_sum(
                when(
                    col("comment").isNotNull(),
                    1
                ).otherwise(0)
            ).alias("nombre_commentaires"),
            (spark_sum(
                    when(
                        col("rating") >= 4,
                        1).otherwise(0))
                /
                count("*")
                * 100
            ).alias("pourcentage_avis_positifs")))

    resume_avis_commandes = (
        resume_avis_commandes
        .withColumn(
            "customer_satisfaction",
            when(
                col("note_moyenne") >= 4,
                "Très satisfait")
            .when(
                col("note_moyenne") >= 3,
                "Satisfait")
            .when(
                col("note_moyenne") >= 2,
                "Peu satisfait")
            .when(
                col("note_moyenne") < 2,
                "Insatisfait")
            .otherwise("Non évalué")))

    jointure_commandes_avis = (
        jointure_commandes_produits
        .join(
            resume_avis_commandes,
            on="order_id_clean",
            how="left"))

    # Étape 32 - Jointure livraisons

    jointure_commandes_livraisons = (
        jointure_commandes_avis
        .join(
            delivery_summary,
            on="order_id_clean",
            how="left")
        .withColumn(
            "delivery_found",
            col("last_delivery_status").isNotNull()))

    # Étape 33 - Création customer_order_360

    customer_order_360 = (
        jointure_commandes_livraisons
        .select(
            "order_id_clean",
            "customer_id_clean",
            "full_name",
            "email_clean",
            "phone_clean",
            "city_clean",
            "country_clean",

            col("o.order_date_clean")
            .alias("order_date_clean"),

            "order_status",
            "payment_method",
            "currency",
            "total_amount_eur",
            "number_of_products",
            "total_quantity",
            "gross_amount_total",
            "discount_total",
            "net_amount_total",
            "amount_difference",
            "liste_produits",
            "liste_categories",
            "liste_marques",
            "nombre_categories_differentes",
            "nombre_avis",
            "note_moyenne",
            "note_min",
            "note_max",
            "pourcentage_avis_positifs",
            "customer_satisfaction",
            "last_delivery_status",
            "carrier_name_clean",
            "first_event",
            "last_event",
            "delivery_delay_days",
            "delivery_performance"))


    # Étape 34 - Score qualité

    customer_order_360 = (
        customer_order_360
        .withColumn(
            "data_quality_score",
            lit(100)
            - when(
                col("amount_difference") > 0.01,
                20
            ).otherwise(0)
            - when(
                col("delivery_delay_days").isNull(),
                10
            ).otherwise(0)
            - when(
                col("email_clean").isNull(),
                10
            ).otherwise(0))
        .withColumn(
            "quality_level",
            when(
                col("data_quality_score") >= 90,
                "Excellent")
            .when(
                col("data_quality_score") >= 75,
                "Bon")
            .when(
                col("data_quality_score") >= 50,
                "Moyen")
            .otherwise("Faible")))

    return (
        customer_order_360,
        commandes_orphelines,
        details_produits_commande,
        resume_avis_commandes)