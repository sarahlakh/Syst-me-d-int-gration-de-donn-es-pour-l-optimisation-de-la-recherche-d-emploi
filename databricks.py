from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col, explode

spark = SparkSession.builder.appName("OffresDatabricks").getOrCreate()


df_offres = spark.read.json("jobsoffer.json")

df_entreprises_raw = spark.read.json("francetravail.json")

df_entreprises = df_entreprises_raw.select(explode("resultats").alias("entreprise")).select("entreprise.*")
df_events = spark.read.csv("events.results.csv", header=True)
df_transports = spark.read.csv("trafic-annuel-entrant-par-station-du-reseau-ferre-2020.csv", sep=";", header=True)


df_offres.createOrReplaceTempView("offres")
df_entreprises.createOrReplaceTempView("entreprise")
df_events.createOrReplaceTempView("networking")
df_transports.createOrReplaceTempView("transports")

def refresh_all_views():
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW offres_view AS
        SELECT o.id, o.intitule, o.description, o.lieu_travail_libelle,
               o.entreprise_nom, e.url AS entreprise_url,
               o.type_contrat, o.experience_exige, o.salaire_libelle, o.secteur
        FROM offres o
        LEFT JOIN entreprise e ON o.entreprise_nom = e.nom
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW offres_networking_view AS
        SELECT o.id, o.intitule, o.secteur, n.nom AS event_name,
               n.url AS event_url, n.date_debut
        FROM offres o
        JOIN networking n ON LOWER(o.secteur) = LOWER(n.categorie)
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW offres_transport_view AS
        SELECT o.id, o.intitule, o.lieu_travail_libelle, t.station, t.trafic
        FROM offres o
        JOIN transports t ON LOWER(o.lieu_travail_libelle) LIKE LOWER(CONCAT('%', t.ville, '%'))
        ORDER BY t.trafic DESC
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW recrutement_networking_view AS
        SELECT o.entreprise_nom, COUNT(o.id) AS nombres_offres, n.nom AS event_name, n.date_debut
        FROM offres o
        JOIN networking n ON LOWER(o.entreprise_nom) LIKE LOWER(CONCAT('%', n.nom, '%'))
        GROUP BY o.entreprise_nom, n.nom, n.date_debut
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW sector_networking_view AS
        SELECT o.secteur, COUNT(o.id) AS nombre_offres, COUNT(n.id) AS nombre_evenements
        FROM offres o
        LEFT JOIN networking n ON LOWER(o.secteur) = LOWER(n.categorie)
        GROUP BY o.secteur
        ORDER BY nombre_offres DESC, nombre_evenements DESC
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW recent_networking_offres_view AS
        SELECT o.id, o.intitule, o.secteur, n.nom AS event_name, n.date_debut
        FROM offres o
        JOIN networking n ON LOWER(o.secteur) = LOWER(n.categorie)
        WHERE n.date_debut >= DATE_SUB(current_date(), 30)
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW company_sector_city_view AS
        SELECT entreprise_nom, COUNT(DISTINCT lieu_travail_libelle) AS nombre_villes,
               COUNT(DISTINCT secteur) AS nombre_secteurs
        FROM offres
        GROUP BY entreprise_nom
        ORDER BY nombre_villes DESC, nombre_secteurs DESC
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW formation_events_view AS
        SELECT o.id, o.intitule, o.entreprise_nom, n.nom AS event_name, n.date_debut, n.url
        FROM offres o
        JOIN networking n ON LOWER(o.entreprise_nom) LIKE LOWER(CONCAT('%', n.nom, '%'))
        WHERE LOWER(n.categorie) LIKE '%formation%'
    """)

refresh_all_views()

spark.sql("SELECT * FROM recent_networking_offres_view").show(10, truncate=False)