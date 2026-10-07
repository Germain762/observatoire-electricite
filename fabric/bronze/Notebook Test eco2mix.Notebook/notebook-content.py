# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5cfb5a02-b6fb-48e7-8e22-3cb56bbb5daf",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "ad90f025-9a04-41a2-b271-9b5881e5e6ad",
# META       "known_lakehouses": [
# META         {
# META           "id": "5cfb5a02-b6fb-48e7-8e22-3cb56bbb5daf"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
df = spark.read.parquet("Files/eco2mix/regional_cons_def")
display(df.select("nature").distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.groupBy("libelle_region").count().orderBy("libelle_region"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.groupBy("date_heure").count().orderBy("date_heure"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType

def indicateur_null(champ):
    cond = F.col(champ.name).isNull()
    if isinstance(champ.dataType, (DoubleType, FloatType)):
        cond = cond | F.isnan(champ.name)   # NaN n'est pas NULL pour Spark
    return cond.cast("int")

taux = df.select([
    F.round(F.avg(indicateur_null(c)) * 100, 2).alias(c.name)
    for c in df.schema.fields
])
display(taux)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

r = taux.first().asDict()
res = (pd.DataFrame({"colonne": list(r.keys()), "taux_null_pct": list(r.values())})
         .sort_values("taux_null_pct", ascending=False))
display(res)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cols = [c for c in df.schema.fields if c.name != "annee"]
display(df.groupBy("annee")
          .agg(*[F.round(F.avg(indicateur_null(c)) * 100, 2).alias(c.name) for c in cols])
          .orderBy("annee"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
