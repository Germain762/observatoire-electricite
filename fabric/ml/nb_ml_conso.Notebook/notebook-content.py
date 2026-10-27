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

import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error
import mlflow, lightgbm as lgb
from sklearn.linear_model import LinearRegression

%pip install holidays -q
import holidays

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_ml_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = (spark.read.table("fact_conso_meteo_horaire")
        .filter("nb_pas = 2")                       # on écarte les heures incomplètes
        .select("code_region", "heure_utc", "heure_paris", "conso_mwh",
                "temperature_c", "vent_kmh", "rayonnement_wm2")
        .toPandas())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df["heure"] = df["heure_paris"].dt.hour                         # la vie suit l'heure locale, pas l'UTC
df["jour_semaine"] = df["heure_paris"].dt.dayofweek             # 0 = lundi
df["mois"] = df["heure_paris"].dt.month
df["chauffage"] = (15 - df["temperature_c"]).clip(lower=0)
df["climatisation"] = (df["temperature_c"] - 22).clip(lower=0)

# consommation de la même heure, 7 jours avant : jointure sur l'horodatage, pas shift()
lag = df[["code_region", "heure_utc", "conso_mwh"]].copy()
lag["heure_utc"] = lag["heure_utc"] + pd.Timedelta(days=7)
df = df.merge(lag.rename(columns={"conso_mwh": "conso_j7"}),
              on=["code_region", "heure_utc"], how="left")
df = df.dropna(subset=["conso_j7", "temperature_c"])
df["code_region"] = df["code_region"].astype("category")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

feries = set(holidays.France(years=range(2021, 2027)).keys())
jour = df["heure_paris"].dt.normalize()

df["ferie"] = jour.dt.date.isin(feries).astype(int)
df["pont"] = (((df["jour_semaine"] == 4) & (jour - pd.Timedelta(days=1)).dt.date.isin(feries)) |
              ((df["jour_semaine"] == 0) & (jour + pd.Timedelta(days=1)).dt.date.isin(feries))).astype(int)
df["ferie_j7"] = (jour - pd.Timedelta(days=7)).dt.date.isin(feries).astype(int)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.sort_values(["code_region", "heure_utc"])

# moyennes glissantes sur une durée (24 h, 72 h), pas sur un nombre de lignes
for h in (24, 72):
    roll = (df.set_index("heure_utc").groupby("code_region", observed=True)["temperature_c"]
              .rolling(f"{h}h").mean().rename(f"temp_moy_{h}h").reset_index())
    df = df.merge(roll, on=["code_region", "heure_utc"])

# écart de température avec la même heure, une semaine avant : pourquoi conso_j7 n'est pas représentatif
t7 = df[["code_region", "heure_utc", "temperature_c"]].copy()
t7["heure_utc"] = t7["heure_utc"] + pd.Timedelta(days=7)
df = df.merge(t7.rename(columns={"temperature_c": "temp_j7"}), on=["code_region", "heure_utc"], how="left")
df["delta_temp_j7"] = df["temperature_c"] - df["temp_j7"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

train = df[df["heure_utc"] < "2024-01-01"]
valid = df[(df["heure_utc"] >= "2024-01-01") & (df["heure_utc"] < "2025-01-01")]
test  = df[(df["heure_utc"] >= "2025-01-01") & (df["heure_utc"] < "2026-01-01")]

FEATURES = ["code_region", "heure", "jour_semaine", "mois", "temperature_c",
            "chauffage", "climatisation", "vent_kmh", "rayonnement_wm2", "conso_j7", 
            "ferie", "pont", "ferie_j7", "delta_temp_j7", "temp_moy_24h", "temp_moy_72h"]

def scores(y, p):
    return {"mae": mean_absolute_error(y, p), "mape": mean_absolute_percentage_error(y, p)}

print("naïf J-7 :", scores(test["conso_mwh"], test["conso_j7"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mlflow.set_experiment("exp_conso")
"""
def prep_lin(d):    # une régression linéaire a besoin de variables numériques : une colonne par région
    return pd.get_dummies(d[FEATURES], columns=["code_region"], dtype=float)

with mlflow.start_run(run_name="regression_lineaire"):
    lin = LinearRegression().fit(prep_lin(train), train["conso_mwh"])
    mlflow.log_metrics(scores(test["conso_mwh"], lin.predict(prep_lin(test))))
"""

train_full = pd.concat([train, valid])
with mlflow.start_run(run_name="lightgbm"):
    gbm = lgb.LGBMRegressor(n_estimators=500, learning_rate=0.05, num_leaves=63)
    gbm.fit(train_full[FEATURES], train_full["conso_mwh"])
    mlflow.log_metrics(scores(test["conso_mwh"], gbm.predict(test[FEATURES])))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

res = test.assign(pred=gbm.predict(test[FEATURES]))
res["err"] = (res["pred"] - res["conso_mwh"]).abs() / res["conso_mwh"]
res["jour"] = res["heure_paris"].dt.date
print(res.groupby("jour")["err"].mean().sort_values(ascending=False).head(10))
print("Jours fériés :", res.loc[res["ferie"] == 1, "err"].mean())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

regions = (spark.read.table("lh_bronze.dbo.ref_regions")
             .filter("perimetre_eco2mix").toPandas())
VARS = ["temperature_2m_previous_day1", "wind_speed_10m_previous_day1",
        "shortwave_radiation_previous_day1"]

URL = "https://previous-runs-api.open-meteo.com/v1/forecast"
s = requests.Session()
s.mount("https://", HTTPAdapter(max_retries=Retry(
    total=4, backoff_factor=5, status_forcelist=[429, 500, 502, 503, 504])))

r0 = regions.iloc[0]
t0 = time.monotonic()
resp = s.get(URL, params={"latitude": r0.latitude, "longitude": r0.longitude,
                          "hourly": ",".join(VARS), "start_date": "2025-01-01",
                          "end_date": "2025-01-07", "timezone": "UTC"}, timeout=120)

frames = []
for r in regions.itertuples():
    for debut in pd.date_range("2024-12-01", "2025-12-01", freq="MS"):
        fin = debut + pd.offsets.MonthEnd(0)
        resp = s.get(URL, params={"latitude": r.latitude, "longitude": r.longitude,
                                  "hourly": ",".join(VARS),
                                  "start_date": debut.strftime("%Y-%m-%d"),
                                  "end_date": fin.strftime("%Y-%m-%d"),
                                  "timezone": "UTC"}, timeout=120)
        resp.raise_for_status()
        h = pd.DataFrame(resp.json()["hourly"])
        h["code_region"] = str(r.code_insee_region)
        frames.append(h)
        time.sleep(1)
    print(r.code_insee_region, "ok")

prev = pd.concat(frames, ignore_index=True)
prev["heure_utc"] = pd.to_datetime(prev["time"])
print(len(prev), prev.isna().mean().round(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

p = prev.rename(columns={"temperature_2m_previous_day1": "temperature_c",
                         "wind_speed_10m_previous_day1": "vent_kmh"})[
        ["code_region", "heure_utc", "temperature_c", "vent_kmh"]]
ray = prev[["code_region", "heure_utc", "shortwave_radiation_previous_day1"]].rename(
        columns={"shortwave_radiation_previous_day1": "rayonnement_wm2"})
ray["heure_utc"] -= pd.Timedelta(hours=1)
p = p.merge(ray, on=["code_region", "heure_utc"], how="left")

# variables de calendrier et conso_j7 de 2025 (connues la veille), météo remplacée par la prévision
CAL = ["code_region", "heure_utc", "heure_paris", "conso_mwh", "heure", "jour_semaine",
       "mois", "conso_j7", "ferie", "pont", "ferie_j7"]
base = test[CAL].astype({"code_region": str})
X_prev = base.merge(variables_meteo(p), on=["code_region", "heure_utc"], how="inner")
X_prev["code_region"] = pd.Categorical(X_prev["code_region"],
                                       categories=test["code_region"].cat.categories)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(len(test), len(X_prev))   # doivent être égaux
print("météo observée :", scores(test["conso_mwh"], gbm.predict(test[FEATURES])))
print("météo prévue J-1 :", scores(X_prev["conso_mwh"], gbm.predict(X_prev[FEATURES])))

# pour situer : l'erreur de la prévision de température elle-même
cmp = (test[["code_region", "heure_utc", "temperature_c"]].astype({"code_region": str})
         .merge(p, on=["code_region", "heure_utc"], suffixes=("_obs", "_prev")))
print("erreur moyenne de température :",
      (cmp["temperature_c_prev"] - cmp["temperature_c_obs"]).abs().mean().round(2), "°C")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(spark.createDataFrame(prev[["code_region", "heure_utc", "temperature_2m_previous_day1",
                             "wind_speed_10m_previous_day1", "shortwave_radiation_previous_day1"]])
   .write.mode("overwrite").saveAsTable("lh_silver.dbo.meteo_prevue_j1"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
