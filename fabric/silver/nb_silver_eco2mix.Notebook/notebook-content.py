# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7fe0803e-20d7-4eb5-8a18-59471bd8cc32",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "ad90f025-9a04-41a2-b271-9b5881e5e6ad",
# META       "known_lakehouses": [
# META         {
# META           "id": "7fe0803e-20d7-4eb5-8a18-59471bd8cc32"
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

ws = notebookutils.runtime.context["currentWorkspaceName"]
BRONZE = f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files"

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

from pyspark.sql  import functions as F, Window
from delta.tables import DeltaTable
import json

spark.conf.set("spark.sql.session.timeZone", "UTC")
mesures = ["consommation", "thermique", "nucleaire", "eolien", "solaire", "hydraulique", "pompage", "bioenergies", "ech_physiques", 
            "stockage_batterie", "destockage_batterie", "eolien_terrestre", "eolien_offshore", "tco_thermique", "tch_thermique", "tco_nucleaire", "tch_nucleaire",
            "tco_eolien", "tch_eolien", "tco_solaire", "tch_solaire", "tco_hydraulique", "tch_hydraulique", "tco_bioenergies", "tch_bioenergies"]

src = (spark.read.parquet(f"{BRONZE}/eco2mix/regional_cons_def/")
         .filter(F.col("annee") >= annee_debut))

# définitif prioritaire sur consolidé : adapte aux valeurs réelles de `nature`
priorite = F.when(F.col("nature") == "Données définitives", 0).otherwise(1)
heure_valide = F.when(
    F.date_format(F.from_utc_timestamp(F.col("date_heure").cast("timestamp"), "Europe/Paris"), "HH:mm")
    == F.col("heure"), 0
).otherwise(1)
w = Window.partitionBy("code_insee_region", "date_heure").orderBy(heure_valide, priorite)

detail_publie = F.col("ts_utc") >= F.lit("2024-01-01").cast("timestamp")

silver = (src
  .withColumn("rn", F.row_number().over(w)).filter("rn = 1")
  .select(F.col("code_insee_region").cast("string"),
          "libelle_region", "nature",
          F.col("date_heure").cast("timestamp").alias("ts_utc"),
          *[F.col(c).cast("double").alias(c) for c in mesures])
  .withColumn("eolien_terrestre", F.when(detail_publie, F.col("eolien_terrestre")))
  .withColumn("eolien_offshore",  F.when(detail_publie, F.col("eolien_offshore")))
  .withColumn("ts_paris", F.from_utc_timestamp("ts_utc", "Europe/Paris"))
  .withColumn("annee", F.year("ts_utc"))
  .withColumn("row_hash", row_hash(["nature", *mesures])))

silver = silver.cache()          # évite de recalculer toute la chaîne pour chaque count
n_src, n_silver = src.count(), silver.count()
n_suppr = n_src - n_silver
print(f"{n_src} lignes lues, {n_suppr} supprimées par dédoublonnage")

if not spark.catalog.tableExists("eco2mix_regional"):
    silver.write.format("delta").saveAsTable("eco2mix_regional")
else:
    (DeltaTable.forName(spark, "eco2mix_regional").alias("t")
       .merge(silver.alias("s"),
              "t.code_insee_region = s.code_insee_region AND t.ts_utc = s.ts_utc")
       .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash")
       .whenNotMatchedInsertAll()
       .execute())

notebookutils.notebook.exit(json.dumps({"lues": n_src, "supprimees": n_suppr}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
