# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import functions as F

def row_hash(cols):
    return F.sha2(F.concat_ws("|", *[
        F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols
    ]), 256)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
def write_slice(df, table, annee_debut):
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        print(f"table {table} créée de {annee_debut}")
    else:
        (df.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"annee >= {annee_debut}")
        .saveAsTable(table))
        print(f"table {table} modifiée avec donéées de {annee_debut}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lever_si_echecs(erreurs, table):
    if erreurs:
        raise ValueError(f"[{table}] contrôles échoués : " + " | ".join(erreurs))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timezone

def log_execution(table_cible, statut, lignes=None, message=None):
    schema = ("run_id string, notebook string, table_cible string, statut string, "
              "lignes long, message string, horodatage_utc timestamp")
    ligne = [(run_id, notebookutils.runtime.context["currentNotebookName"], table_cible,
              statut, lignes, message, datetime.now(timezone.utc))]
    spark.createDataFrame(ligne, schema).write.mode("append").saveAsTable("lh_admin.dbo.log_execution")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

def publier(df, table, controles, annee_debut):
    df = df.cache()
    try:
        lever_si_echecs(controles(df), table)
        write_slice(df, table, annee_debut)
        n = int(DeltaTable.forName(spark, table).history(1)
                  .select("operationMetrics.numOutputRows").first()[0])
        log_execution(table, "succes", n)
        return n
    except Exception as e:
        log_execution(table, "echec", message=str(e)[:1000])
        raise
    finally:
        df.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
