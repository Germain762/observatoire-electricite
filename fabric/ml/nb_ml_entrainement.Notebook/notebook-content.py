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

%pip install holidays -q
import holidays
import mlflow, lightgbm as lgb

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

df = construire_variables(charger_conso(), meteo_observee())
X, y = preparer_X(df), df["conso_mwh"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

train = df[df["heure_utc"] < "2025-01-01"]
test  = df[(df["heure_utc"] >= "2025-01-01") & (df["heure_utc"] < "2026-01-01")]

gbm_verif = lgb.LGBMRegressor(n_estimators=500, learning_rate=0.05, num_leaves=63)
gbm_verif.fit(preparer_X(train), train["conso_mwh"], categorical_feature=["region_id"])
print(scores(test["conso_mwh"], gbm_verif.predict(preparer_X(test))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from mlflow.models.signature import infer_signature

mlflow.autolog(disable=True)           # on enregistre le modèle nous-mêmes, une seule fois
mlflow.set_experiment("exp_conso")

X, y = preparer_X(df), df["conso_mwh"]
with mlflow.start_run(run_name="lightgbm_production"):
    gbm = lgb.LGBMRegressor(n_estimators=500, learning_rate=0.05, num_leaves=63)
    gbm.fit(X, y, categorical_feature=["region_id"])
    mlflow.lightgbm.log_model(gbm, "model",
                              signature=infer_signature(X, gbm.predict(X)),
                              registered_model_name="ml_conso")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
