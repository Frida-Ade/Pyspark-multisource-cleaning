-- 1. Chiffre d'affaires mensuel par pays

SELECT
    country_clean,
    order_year,
    order_month,
    ROUND(SUM(total_amount_eur),2) AS chiffre_affaires
FROM customer_order_360_view
GROUP BY country_clean, order_year, order_month
ORDER BY country_clean, order_year, order_month;

-- 2. Top 5 catégories générant le plus de CA

SELECT
    categorie,
    ROUND(SUM(total_amount_eur),2) AS chiffre_affaires
FROM (
    SELECT
        explode(liste_categories) AS categorie,
        total_amount_eur
    FROM customer_order_360_view
)
GROUP BY categorie
ORDER BY chiffre_affaires DESC
LIMIT 5;

-- 3. Top 10 clients ayant le plus dépensé

SELECT
    customer_id_clean,
    full_name,
    ROUND(SUM(total_amount_eur),2) AS montant_total
FROM customer_order_360_view
GROUP BY customer_id_clean, full_name
ORDER BY montant_total DESC
LIMIT 10;

-- 4. Note moyenne par catégorie

SELECT
    categorie,
    ROUND(AVG(note_moyenne),2) AS note_moyenne_categorie
FROM (
    SELECT
        explode(liste_categories) AS categorie,
        note_moyenne
    FROM customer_order_360_view
    WHERE note_moyenne IS NOT NULL
)
GROUP BY categorie
ORDER BY note_moyenne_categorie DESC;

-- 5. Transporteur avec le délai moyen le plus faible

SELECT
    carrier_name_clean,
    ROUND(AVG(delivery_delay_days),2) AS delai_moyen
FROM customer_order_360_view
WHERE delivery_delay_days IS NOT NULL
GROUP BY carrier_name_clean
ORDER BY delai_moyen ASC;

-- 6. Pourcentage de commandes livrées en moins de 3 jours

SELECT
    ROUND(
        100.0 *
        SUM(
            CASE
                WHEN delivery_delay_days < 3 THEN 1
                ELSE 0
            END
        ) /
        COUNT(*),
        2
    ) AS pourcentage_livraison_rapide
FROM customer_order_360_view
WHERE delivery_delay_days IS NOT NULL;

-- 7. Clients ayant au moins 3 commandes et une note moyenne < 3

SELECT
    customer_id_clean,
    full_name,
    COUNT(*) AS nb_commandes,
    ROUND(AVG(note_moyenne),2) AS note_moyenne_client
FROM customer_order_360_view
WHERE note_moyenne IS NOT NULL
GROUP BY customer_id_clean, full_name
HAVING COUNT(*) >= 3
   AND AVG(note_moyenne) < 3
ORDER BY note_moyenne_client;

-- 8. Commandes dont le montant déclaré diffère du montant recalculé

SELECT
    order_id_clean,
    total_amount_eur,
    net_amount_total,
    amount_difference
FROM customer_order_360_view
WHERE amount_difference > 0.01
ORDER BY amount_difference DESC;

-- 9. Pourcentage de commandes orphelines

SELECT
    ROUND(
        100.0 *
        SUM(
            CASE
                WHEN full_name IS NULL THEN 1
                ELSE 0
            END
        ) / COUNT(*),
        2
    ) AS pourcentage_commandes_orphelines
FROM customer_order_360_view;

-- 10. Répartition des commandes selon le niveau de qualité

SELECT
    quality_level,
    COUNT(*) AS nb_commandes,
    ROUND(
        100.0 * COUNT(*) /
        (SELECT COUNT(*) FROM customer_order_360_view),
        2
    ) AS pourcentage
FROM customer_order_360_view
GROUP BY quality_level
ORDER BY nb_commandes DESC;