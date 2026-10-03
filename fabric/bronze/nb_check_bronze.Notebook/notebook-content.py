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
display(df.groupBy("annee", "libelle_region").count().orderBy("annee", "libelle_region"))
#assert(df_eco2mix.groupBy("annee", "libelle_region"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

ref = spark.read.table("ref_regions").select("libelle_region")
regions_eco = df.select("libelle_region").distinct()

# Régions du référentiel absentes d'éCO2mix
manquantes = ref.join(regions_eco, "libelle_region", "left_anti")
# Régions d'éCO2mix absentes du référentiel (souvent un souci de libellé)
inconnues = regions_eco.join(ref, "libelle_region", "left_anti")

display(manquantes)
display(inconnues)

# Nombre de régions distinctes par année
par_annee = df.groupBy("annee").agg(F.countDistinct("libelle_region").alias("nb_regions"))
display(par_annee.orderBy("annee"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

eco = (spark.read.parquet("Files/eco2mix/regional_cons_def/")   # la colonne annee vient du dossier annee=
          .select(F.col("code_insee_region").cast("string"), "annee")
          .distinct())

ref = (spark.read.table("ref_regions")
          .filter("perimetre_eco2mix")
          .select(F.col("code_insee_region").cast("string")))

attendu = ref.crossJoin(eco.select("annee").distinct())
manquant = attendu.join(eco, ["code_insee_region", "annee"], "left_anti")

n = manquant.count()
if n > 0:
    display(manquant)
    raise Exception(f"{n} combinaison(s) région/année absente(s) du bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
