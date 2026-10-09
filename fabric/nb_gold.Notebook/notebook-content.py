# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f79e1413-8920-4c89-ac6e-630513ec0f5e",
# META       "default_lakehouse_name": "lh_gold",
# META       "default_lakehouse_workspace_id": "ad90f025-9a04-41a2-b271-9b5881e5e6ad",
# META       "known_lakehouses": [
# META         {
# META           "id": "f79e1413-8920-4c89-ac6e-630513ec0f5e"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "ee264c39-2ea9-46c2-8f17-ac102772ae71",
# META       "known_warehouses": [
# META         {
# META           "id": "ee264c39-2ea9-46c2-8f17-ac102772ae71",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

annee_debut = 2021
run_id = "manuel"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

FILIERES = ["thermique", "nucleaire", "eolien", "solaire", "hydraulique", "bioenergies", "destockage_batterie"]
COLONNES_METEO = ["temperature_c", "vent_kmh", "rayonnement_wm2"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

metriques = {}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Ingestion dans la table fact_production_horaire

# CELL ********************

eco2mix = spark.read.table("lh_silver.dbo.eco2mix_regional").filter(F.year("ts_utc") >= annee_debut)
filiere_mw = eco2mix.unpivot(["code_insee_region", "ts_utc"], FILIERES, "filiere", "mw").withColumnRenamed("code_insee_region", "code_region")
filiere_mw = filiere_mw.filter(F.col("mw").isNotNull())
#display(filiere_mw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filiere_to_mwh = (
    filiere_mw.groupBy("code_region", F.date_trunc("hour", "ts_utc").alias("heure_utc"), "filiere")
            .agg((F.sum("mw") * 0.5).alias("mwh"),  F.count("mw").alias("nb_pas"))
            .withColumn("annee", F.year("heure_utc"))
            .withColumn("heure_paris", F.from_utc_timestamp("heure_utc", "Europe/Paris"))
)

#display(filiere_to_mwh.orderBy("code_region", "heure_utc"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def controles_production(df):
    erreurs = []
    if df.groupBy("code_region", "heure_utc", "filiere").count().filter("count > 1").limit(1).count() > 0:
        erreurs.append("doublons sur la clé")

    SEUIL_HEURES_INCOMPLETES = 24   # nombre de couples (région, heure), à ajuster

    ko = df.filter("nb_pas <> 2").select("code_region", "heure_utc").distinct()
    n = ko.count()
    if n > 0:
        exemples = [f"{r.code_region} {r.heure_utc}" for r in ko.limit(5).collect()]
        msg = f"{n} heure(s) incomplète(s) à la source, ex. {exemples}"
        if n > SEUIL_HEURES_INCOMPLETES:
            erreurs.append(msg)
        else:
            log_execution("fact_production_horaire", "avertissement", n, msg)

    INTERDITES = {"consommation", "pompage", "stockage_batterie", "ech_physiques",
              "eolien_terrestre", "eolien_offshore"}

    invalides = (set(FILIERES) & INTERDITES) | {f for f in FILIERES if f.startswith(("tco_", "tch_"))}
    if invalides:
        erreurs.append(f"FILIERES contient des colonnes interdites : {sorted(invalides)}")

    total_gold   = df.agg(F.sum("mwh")).first()[0]
    total_silver = sum(v for v in eco2mix.agg(*[F.sum(c) for c in FILIERES]).first() if v) * 0.5
    if abs(total_gold - total_silver) > 1:
        erreurs.append(f"conservation : gold {total_gold:.0f} vs silver {total_silver:.0f}")

    return erreurs

metriques["fact_production_horaire"] = publier(filiere_to_mwh, "fact_production_horaire",
                                               controles_production, annee_debut)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

publier(filiere_to_mwh, "fact_production_horaire", controles_production, annee_debut)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Ingestion dans la table fact_conso_meteo_horaire

# CELL ********************

meteo = spark.read.table("lh_silver.dbo.meteo_horaire")
instantane = meteo.select("code_insee_region", "ts_utc", "temperature_c", "vent_kmh")

# rayonnement horodaté h+1 (moyenne de h à h+1) ramené sur l'heure h
rayonnement = meteo.select(
    "code_insee_region",
    F.timestamp_seconds(F.unix_timestamp("ts_utc") - 3600).alias("ts_utc"),
    "rayonnement_wm2")

consommation = (
    eco2mix.groupBy("code_insee_region", F.date_trunc("hour", "ts_utc").alias("ts_utc"))
            .agg((F.sum("consommation") * 0.5).alias("conso_mwh"), F.count("consommation").alias("nb_pas")
    )
)
#display(consommation.orderBy("code_insee_region", "ts_utc"))                            

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

jointure = (consommation
                .join(instantane, ["code_insee_region", "ts_utc"], "left")
                .join(rayonnement,["code_insee_region", "ts_utc"], "left")
                .withColumn("annee",  F.year("ts_utc"))
                .withColumn("heure_paris", F.from_utc_timestamp("ts_utc", "Europe/Paris"))
                .select("code_insee_region",
                    "ts_utc", "conso_mwh", "nb_pas", "temperature_c", "vent_kmh", "rayonnement_wm2", "annee", "heure_paris")
                .withColumnsRenamed({
                    "code_insee_region": "code_region", 
                    "ts_utc": "heure_utc"})
            )
#display(jointure)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def controles_consommation(df):
    erreurs = []
    n_avant, n_apres = consommation.count(), df.count()
    if n_apres == 0:
        erreurs.append("table vide")
        
    if n_apres != n_avant:
        erreurs.append(f"la jointure météo a changé le nombre de lignes : {n_avant} → {n_apres}")

    if df.groupBy("code_region", "heure_utc").count().filter("count > 1").limit(1).count() > 0:
        erreurs.append("doublons sur la clé (région, heure)")

    SEUIL_HEURES_INCOMPLETES = 24   # nombre de couples (région, heure), à ajuster

    ko = df.filter("nb_pas <> 2").select("code_region", "heure_utc").distinct()
    n = ko.count()
    if n > 0:
        exemples = [f"{r.code_region} {r.heure_utc}" for r in ko.limit(5).collect()]
        msg = f"{n} heure(s) incomplète(s) à la source, ex. {exemples}"
        if n > SEUIL_HEURES_INCOMPLETES:
            erreurs.append(msg)
        else:
            log_execution("fact_conso_meteo_horaire", "avertissement", n, msg)

    nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
                       for c in COLONNES_METEO]).first().asDict()
    for colonne, n in nulls.items():
        if n:
            erreurs.append(f"{n} null(s) dans {colonne}")

    n_attendu = spark.read.table("lh_bronze.dbo.ref_regions").filter("perimetre_eco2mix").count()
    n_regions = df.select("code_region").distinct().count()
    if n_regions != n_attendu:
        erreurs.append(f"{n_regions} régions présentes, {n_attendu} attendues")

    return erreurs

metriques["fact_conso_meteo_horaire"] = publier(jointure, "fact_conso_meteo_horaire",
                                               controles_consommation, annee_debut)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

publier(jointure, "fact_conso_meteo_horaire", controles_consommation, annee_debut)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Ingestion dans la table agg_mix_journalier

# CELL ********************

journalier = (spark.read.table("fact_production_horaire")
                .withColumn("date_locale", F.to_date(F.from_utc_timestamp("heure_utc", "Europe/Paris")))
                .filter(F.year("date_locale") >= annee_debut)
                .groupBy("code_region", "date_locale", "filiere")
                .agg(F.sum("mwh").alias("mwh"), F.count("*").alias("nb_heures")))
display(journalier)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w = Window.partitionBy("code_region", "date_locale")

agg_mix = (journalier
            .withColumn("mwh_total_jour", F.sum("mwh").over(w))
            .withColumn("part", F.when(F.col("mwh_total_jour") != 0, F.col("mwh") / F.col("mwh_total_jour")))
            .withColumn("annee", F.year("date_locale")))
display(agg_mix)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def controles_mix(df):
    erreurs = []

    if df.groupBy("code_region", "date_locale", "filiere").count().filter("count > 1").limit(1).count() > 0:
        erreurs.append("doublons sur la clé (code_region, date_locale, filiere)")

    debut_jour = F.to_utc_timestamp(F.col("date_locale").cast("timestamp"), "Europe/Paris")
    fin_jour   = F.to_utc_timestamp(F.date_add("date_locale", 1).cast("timestamp"), "Europe/Paris")
    heures_civiles = (F.unix_timestamp(fin_jour) - F.unix_timestamp(debut_jour)) / 3600   # 23, 24 ou 25

    premier_jour = (spark.read.table("lh_gold.dbo.fact_production_horaire")
                    .agg(F.to_date(F.from_utc_timestamp(F.min("heure_utc"), "Europe/Paris")))
                    .first()[0])

    heures_ko = (df.groupBy("code_region", "date_locale")
                .agg(F.max("nb_heures").alias("nb_heures"))
                .withColumn("attendu", F.least(heures_civiles, F.lit(24)))   # la source publie 24 h en octobre
                .filter((F.col("nb_heures") != F.col("attendu"))
                        & (F.col("date_locale") != F.lit(premier_jour))))
                        
    n = heures_ko.count()
    if n > 0:
        exemples = [f"{r.code_region} {r.date_locale} ({r.nb_heures} h au lieu de {r.attendu:.0f})" for r in heures_ko.limit(5).collect()]
        erreurs.append(f"heures : {n} journée(s) incomplète(s), ex. {exemples}")

    #Controle des parts totales
    part_totale = (
        df.groupBy("date_locale", "code_region")
        .agg(F.sum("part").alias("part_totale"))
        .filter(F.col("part_totale").isNull() | (F.abs(F.col("part_totale") - 1) > 1e-9))
    )
    n = part_totale.count()
    if n > 0:
        exemples = [f"{r.date_locale} {row.code_region}: la part totale est de {row.part_totale}" for r in part_totale.limit(5).collect()]
        erreurs.append(f"parts totales : {n} jour(s) en écart, ex. {exemples}")

    #Controle de conservation du même nombre de mwh entre source et calcul
    mwh_attendu = (
        spark.read.table("lh_gold.dbo.fact_production_horaire")
        .withColumn("date_locale", F.to_date(F.from_utc_timestamp("heure_utc", "Europe/Paris")))
        .filter(F.year("date_locale") >= annee_debut)
        .groupBy("date_locale")
        .agg(F.sum("mwh").alias("mwh_attendu"))
    )

    mwh_quotidien = (
        df.groupBy("date_locale")
        .agg(F.sum("mwh").alias("mwh_produit"))
    )

    ecarts = (
        mwh_quotidien
        .join(mwh_attendu, on="date_locale", how="full_outer")
        .filter(
            F.col("mwh_produit").isNull()
            | F.col("mwh_attendu").isNull()
            | (F.abs(F.col("mwh_produit") - F.col("mwh_attendu")) > 1e-9)
        )
    )

    n = ecarts.count()
    if n > 0:
        exemples = [f"{r.date_locale} ({r.mwh_produit} vs {r.mwh_attendu})" for r in ecarts.limit(5).collect()]
        erreurs.append(f"conservation : {n} jour(s) en écart, ex. {exemples}")

    return erreurs

metriques["agg_mix_journalier"] = publier(agg_mix, "agg_mix_journalier",
                                               controles_mix, annee_debut)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

publier(agg_mix, "agg_mix_journalier", controles_mix, annee_debut)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.exit(json.dumps(metriques))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
