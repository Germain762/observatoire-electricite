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
# META     }
# META   }
# META }

# CELL ********************

%run nb_ml_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from synapse.ml.predict import MLFlowTransformer
from pyspark.sql import functions as F
import mlflow

spark.conf.set("spark.sql.session.timeZone", "UTC")

CLES = ["code_region", "heure_utc", "heure_paris", "conso_mwh"]

d = construire_variables(charger_conso(), meteo_prevue())
d = d[(d["heure_utc"] >= "2025-01-01") & (d["heure_utc"] < "2026-01-01")]
print(len(d))      # environ 105 084 attendu, comme en phase 2

entree_pd = d[CLES].join(preparer_X(d))
entree = spark.createDataFrame(entree_pd)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

modele = MLFlowTransformer(inputCols=FEATURES_MODELE, outputCol="prevision_mwh",
                           modelName="ml_conso", modelVersion=1)

sortie = (modele.transform(entree)
    .withColumn("date_key", F.date_format("heure_paris", "yyyyMMdd").cast("int"))
    .withColumn("source_meteo", F.lit("prevue_j1"))
    .withColumn("modele", F.lit("ml_conso/1"))
    .select("code_region", "date_key", "heure_utc", "heure_paris",
            "conso_mwh", "prevision_mwh", "source_meteo", "modele"))

sortie.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_prevision_conso")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

t = spark.read.table("fact_prevision_conso").toPandas()
print(len(t), scores(t["conso_mwh"], t["prevision_mwh"]))

# le même modèle chargé directement par MLflow, sans Spark : les prévisions doivent être identiques
direct = entree_pd[["code_region", "heure_utc"]].assign(
    direct=mlflow.pyfunc.load_model("models:/ml_conso/1").predict(preparer_X(d)))
cmp = t.astype({"code_region": str}).merge(direct, on=["code_region", "heure_utc"])
print("écart maximal :", (cmp["prevision_mwh"] - cmp["direct"]).abs().max())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
