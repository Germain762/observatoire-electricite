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

annee_debut = 2025
run_id = "manuel"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ws = notebookutils.runtime.context["currentWorkspaceName"]
BRONZE = f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files"
spark.conf.set("spark.sql.session.timeZone", "UTC")


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

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

raw = spark.read.option("multiline", True).json(f"{BRONZE}/meteo/")

meteo = (raw
    .select(F.col("region").cast("string").alias("code_insee_region"), "hourly.*")
    .select("code_insee_region",
            F.explode(F.arrays_zip("time", "temperature_2m",
                                    "wind_speed_10m", "shortwave_radiation")).alias("z"))
    .select("code_insee_region",
        F.to_timestamp("z.time").alias("ts_utc"),
        F.col("z.temperature_2m").alias("temperature_c"),
        F.col("z.wind_speed_10m").alias("vent_kmh"),
        F.col("z.shortwave_radiation").alias("rayonnement_wm2")
    )
    .withColumn("row_hash", row_hash(["temperature_c", "vent_kmh", "rayonnement_wm2"]))
)


if not spark.catalog.tableExists("meteo_horaire"):
    meteo.write.format("delta").saveAsTable("meteo_horaire")
else:
    (DeltaTable.forName(spark, "meteo_horaire").alias("t")
       .merge(meteo.alias("m"),
              "t.code_insee_region = m.code_insee_region AND t.ts_utc = m.ts_utc")
       .whenMatchedUpdateAll(condition="t.row_hash <> m.row_hash")
       .whenNotMatchedInsertAll()
       .execute())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
