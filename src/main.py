from extract import (
    create_spark_session,
    load_relational_data,
    load_reviews_data,
    load_delivery_events
)

from cleaning import (
    clean_customers,
    clean_orders,
    clean_order_items,
    clean_products,
    clean_reviews,
    clean_delivery_events
)

from transformations import (
    create_customer_order_360
)

from quality import (
    create_validation_results,
    create_rejects
)

from load import (
    save_outputs,
    validate_saved_outputs
)


def main():

    print("=== DEMARRAGE DU PIPELINE ===")

    # Spark Session

    spark = create_spark_session()

    # Extraction

    print("Chargement des données...")

    (
        customers_df,
        orders_df,
        order_items_df,
        products_df
    ) = load_relational_data(spark)

    reviews_df = load_reviews_data(spark)

    delivery_events_df = load_delivery_events(spark)

    # Nettoyage clients

    (
        customers_clean,
        customers_rejects_step6,
        customers_rejects_step11
    ) = clean_customers(customers_df)

    # Nettoyage commandes

    (
        orders_clean,
        orders_rejects_step13,
        orders_rejects_step15,
        exchange_rates
    ) = clean_orders(
        orders_df,
        spark
    )

    # Nettoyage lignes commandes

    (
        order_items_step16,
        order_items_summary,
        amount_comparison,
        order_items_rejects_step16
    ) = clean_order_items(
        order_items_df,
        orders_clean
    )

    # Nettoyage produits

    products_clean = clean_products(
        products_df
    )

    # Nettoyage avis

    (
        reviews_clean,
        reviews_rejects_step20
    ) = clean_reviews(
        reviews_df,
        orders_clean
    )

    # Nettoyage événements livraison

    delivery_summary = clean_delivery_events(
        delivery_events_df,
        orders_clean
    )

    # Construction Customer 360

    (
        customer_order_360,
        commandes_orphelines,
        details_produits_commande,
        resume_avis_commandes
    ) = create_customer_order_360(
        orders_clean,
        customers_clean,
        order_items_step16,
        order_items_summary,
        products_clean,
        reviews_clean,
        delivery_summary
    )

    # Contrôles qualité

    validation_results = create_validation_results(
        customer_order_360,
        spark
    )

    # Création table rejets

    table_rejets = create_rejects(
        orders_rejects_step13,
        orders_rejects_step15,
        order_items_rejects_step16,
        reviews_rejects_step20
    )

    # Sauvegarde

    save_outputs(
        customer_order_360,
        validation_results,
        table_rejets
    )

    # Vérification lecture Parquet

    validate_saved_outputs(
        spark,
        customer_order_360
    )

    print("=== PIPELINE TERMINE AVEC SUCCES ===")

    spark.stop()


if __name__ == "__main__":
    main()