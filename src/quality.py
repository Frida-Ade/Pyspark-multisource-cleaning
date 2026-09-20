from pyspark.sql.functions import *


def create_validation_results(customer_order_360, spark):

    # Étape 35 - Contrôles qualité

    nb_doublons = (
        customer_order_360
        .groupBy("order_id_clean")
        .count()
        .filter(col("count") > 1)
        .count())

    nb_montants_negatifs = (
        customer_order_360
        .filter(col("total_amount_eur") < 0)
        .count())

    nb_notes_invalides = (
        customer_order_360
        .filter(
            col("note_moyenne").isNotNull()
            &
            ((col("note_moyenne") < 1)
                |
                (col("note_moyenne") > 5)))
        .count())

    nb_scores_invalides = (
        customer_order_360
        .filter(
            (col("data_quality_score") < 0)
            |
            (col("data_quality_score") > 100))
        .count())

    nb_dates_invalides = (
        customer_order_360
        .filter(
            col("last_event").isNotNull()
            &
            (
                col("last_event")
                < col("order_date_clean")))
        .count())

    nb_annulees_livrees = (
        customer_order_360
        .filter(
            (col("order_status") == "CANCELLED")
            &
            (
                col("last_delivery_status")
                == "DELIVERED"))
        .count())

    nb_livrees_sans_date = (
        customer_order_360
        .filter(
            (col("last_delivery_status") == "DELIVERED")
            &
            col("last_event").isNull()).count())

    nb_statuts_invalides = (
        customer_order_360
        .filter(
            ~col("order_status").isin(
                "CREATED",
                "PAID",
                "PREPARING",
                "SHIPPED",
                "DELIVERED",
                "CANCELLED",
                "RETURNED",
                "UNKNOWN")).count())

    validation_results = spark.createDataFrame(
        [
            ("Unicité des commandes", nb_doublons),
            ("Montants négatifs", nb_montants_negatifs),
            ("Notes hors intervalle [1,5]", nb_notes_invalides),
            ("Scores qualité invalides", nb_scores_invalides),
            ("Livraison avant commande", nb_dates_invalides),
            ("Commandes annulées livrées", nb_annulees_livrees),
            ("Livrées sans date de livraison", nb_livrees_sans_date),
            ("Statuts non normalisés", nb_statuts_invalides)
        ],
        ["controle", "nombre_anomalies"])

    validation_results = (
        validation_results
        .withColumn(
            "statut",
            when(
                col("nombre_anomalies") == 0,
                "OK"
            ).otherwise("ERREUR")))

    return validation_results


def create_rejects(
    orders_rejects_step13,
    orders_rejects_step15,
    order_items_rejects_step16,
    reviews_rejects_step20
):

    # Étape 36 - Zone de rejet

    orders_rejects_step15 = (
        orders_step15
        .filter(
            (col("total_amount") <= 0)
            |
            col("currency").isNull()
        )
        .select(
            lit("orders").alias("source"),
            lit("Montant ou devise invalide").alias("rejection_reason"),
            current_timestamp().alias("rejection_timestamp"),
            to_json(
                struct([col(c) for c in orders_step15.columns])
            ).alias("original_data")))
    
    table_rejets = (
        orders_rejects_step13
        .unionByName(orders_rejects_step15)
        .unionByName(order_items_rejects_step16)
        .unionByName(reviews_rejects_step20))

    return table_rejets