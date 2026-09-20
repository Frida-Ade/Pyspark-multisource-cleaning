# TP MultiSource

## Contexte

Une entreprise de commerce électronique possède des données réparties dans trois systèmes différents : une base relationnelle (clients, commandes, produits), une base MongoDB (avis clients) et des fichiers JSON (événements de livraison). Ces données présentent de nombreux problèmes de qualité : identifiants incohérents, doublons, dates dans des formats différents, valeurs manquantes, montants invalides.

## Description

Projet de nettoyage et consolidation de données multisources réalisé avec PySpark.

L'objectif est de consolider plusieurs sources de données hétérogènes afin de construire une vue analytique unique nommée **customer_order_360**.

---

## Sources de données utilisées

### PostgreSQL

- customers
- orders
- order_items
- products

### MongoDB

- reviews

### JSON

- delivery_events

---

## Fonctionnalités réalisées

### Extraction

- Lecture des données PostgreSQL via JDBC
- Lecture des données MongoDB
- Lecture des événements JSON

### Nettoyage

- Normalisation des identifiants
- Validation des emails
- Validation des téléphones
- Nettoyage des villes et pays
- Validation des dates
- Contrôle des montants
- Contrôle des notes clients
- Déduplication des données

### Transformations

- Agrégation des commandes
- Agrégation des produits
- Agrégation des avis
- Agrégation des livraisons
- Construction de la vue Customer Order 360

### Qualité des données

- Création des rapports de validation
- Création d'une table de rejets
- Calcul d'un score de qualité

### Stockage

- Sauvegarde au format Parquet
- Partitionnement par année et mois

---

## Structure du projet

```text
tp_multisource/
│
├── src/
│   ├── main.py
│   ├── extract.py
│   ├── cleaning.py
│   ├── transformations.py
│   ├── quality.py
│   └── load.py
│
├── config/
│   └── application.conf
│
├── sql/
│   ├── create_tables.sql
│   └── analytical_queries.sql
│
├── requirements.txt
└── README.md
```

*Les dossiers `data/` (sources) et `output/` (résultats Parquet, rapports qualité, rejets) sont générés localement à l'exécution et ne sont pas versionnés.*

---

## Modules

### extract.py

Chargement des données depuis :

- PostgreSQL
- MongoDB
- JSON

### cleaning.py

Nettoyage et normalisation :

- Customers
- Orders
- Order Items
- Products
- Reviews
- Delivery Events

### transformations.py

Construction de :

- order_items_summary
- delivery_summary
- reviews_summary
- customer_order_360

### quality.py

Production de :

- validation_results
- table_rejets

### load.py

Sauvegarde et relecture des fichiers Parquet.

### main.py

Orchestration complète du pipeline.

---

## Dépendances

```text
pyspark
pymongo
psycopg2-binary
```

---

## Exécution

```bash
spark-submit \
--master local[*] \
--packages \
org.postgresql:postgresql:42.7.3,\
org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
src/main.py
```

---

## Livrables

Le projet contient :

- Code source PySpark modulaire
- Scripts SQL
- Configuration applicative
- Rapport qualité
- Table des rejets
