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

# CELL ********************

%pip install holidays -q
import holidays
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

REGIONS = ["11", "24", "27", "28", "32", "44", "52", "53", "75", "76", "84", "93"]   # ordre figé
FEATURES_MODELE = ["region_id", "heure", "jour_semaine", "mois", "ferie", "pont", "ferie_j7",
                   "conso_j7", "temperature_c", "chauffage", "climatisation", "vent_kmh",
                   "rayonnement_wm2", "temp_moy_24h", "temp_moy_72h", "delta_temp_j7"]
FERIES = set(holidays.France(years=range(2021, 2031)).keys())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def charger_conso():
    return (spark.read.table("lh_gold.dbo.fact_conso_meteo_horaire")
              .filter("nb_pas = 2")
              .select("code_region", "heure_utc", "heure_paris", "conso_mwh")
              .toPandas().astype({"code_region": str}))

def meteo_observee():
    return (spark.read.table("lh_gold.dbo.fact_conso_meteo_horaire")
              .select("code_region", "heure_utc", "temperature_c", "vent_kmh", "rayonnement_wm2")
              .toPandas().astype({"code_region": str}))

def meteo_prevue():
    p = (spark.read.table("lh_silver.dbo.meteo_prevue_j1").toPandas()
           .astype({"code_region": str})
           .rename(columns={"temperature_2m_previous_day1": "temperature_c",
                            "wind_speed_10m_previous_day1": "vent_kmh",
                            "shortwave_radiation_previous_day1": "rayonnement_wm2"}))
    ray = p[["code_region", "heure_utc", "rayonnement_wm2"]].copy()
    ray["heure_utc"] -= pd.Timedelta(hours=1)          # même alignement que dans le gold
    return p.drop(columns="rayonnement_wm2").merge(ray, on=["code_region", "heure_utc"], how="left")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def variables_calendrier(d):
    d = d.copy()
    jour = d["heure_paris"].dt.normalize()
    d["heure"] = d["heure_paris"].dt.hour
    d["jour_semaine"] = d["heure_paris"].dt.dayofweek
    d["mois"] = d["heure_paris"].dt.month
    d["ferie"] = jour.dt.date.isin(FERIES).astype(int)
    d["pont"] = (((d["jour_semaine"] == 4) & (jour - pd.Timedelta(days=1)).dt.date.isin(FERIES)) |
                 ((d["jour_semaine"] == 0) & (jour + pd.Timedelta(days=1)).dt.date.isin(FERIES))).astype(int)
    d["ferie_j7"] = (jour - pd.Timedelta(days=7)).dt.date.isin(FERIES).astype(int)
    return d

def ajouter_conso_j7(d):
    lag = d[["code_region", "heure_utc", "conso_mwh"]].copy()
    lag["heure_utc"] += pd.Timedelta(days=7)
    return d.merge(lag.rename(columns={"conso_mwh": "conso_j7"}),
                   on=["code_region", "heure_utc"], how="left")

def variables_meteo(m):
    """m : code_region, heure_utc, temperature_c, vent_kmh, rayonnement_wm2"""
    m = m.sort_values(["code_region", "heure_utc"]).copy()
    m["chauffage"] = (15 - m["temperature_c"]).clip(lower=0)
    m["climatisation"] = (m["temperature_c"] - 22).clip(lower=0)
    for h in (24, 72):
        roll = (m.set_index("heure_utc").groupby("code_region")["temperature_c"]
                  .rolling(f"{h}h").mean().rename(f"temp_moy_{h}h").reset_index())
        m = m.merge(roll, on=["code_region", "heure_utc"])
    t7 = m[["code_region", "heure_utc", "temperature_c"]].copy()
    t7["heure_utc"] += pd.Timedelta(days=7)
    m = m.merge(t7.rename(columns={"temperature_c": "temp_j7"}), on=["code_region", "heure_utc"], how="left")
    m["delta_temp_j7"] = m["temperature_c"] - m["temp_j7"]
    return m

def construire_variables(conso, meteo):
    d = ajouter_conso_j7(variables_calendrier(conso))
    d = d.merge(variables_meteo(meteo), on=["code_region", "heure_utc"], how="inner")
    return d.dropna(subset=["conso_j7", "temperature_c", "temp_moy_72h"])

def preparer_X(d):
    X = d.copy()
    X["region_id"] = X["code_region"].astype(str).map(REGIONS.index)
    return X[FEATURES_MODELE].astype("float64")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def scores(y, p):
    return {"mae": mean_absolute_error(y, p), "mape": mean_absolute_percentage_error(y, p)}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
