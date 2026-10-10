# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

lakehouses = "lh_gold,lh_admin"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
import sempy.fabric as fabric

client = fabric.FabricRestClient()
ws_id = fabric.get_workspace_id()

def chemin(url: str) -> str:
    # FabricRestClient attend un chemin relatif ; l'en-tête Location renvoie une URL absolue
    return url.split("api.fabric.microsoft.com/")[-1]

def sql_endpoint_id(nom: str) -> str:
    items = client.get(f"v1/workspaces/{ws_id}/lakehouses").json()["value"]
    lh = next(l for l in items if l["displayName"] == nom)
    return lh["properties"]["sqlEndpointProperties"]["id"]

def rafraichir(nom: str) -> None:
    r = client.post(f"v1/workspaces/{ws_id}/sqlEndpoints/{sql_endpoint_id(nom)}/refreshMetadata", json={})
    if r.status_code == 202:                       # opération longue : on attend la fin
        operation = chemin(r.headers["Location"])
        while True:
            time.sleep(int(r.headers.get("Retry-After", 5)))
            r = client.get(operation)
            etat = r.json().get("status")
            if etat in ("Succeeded", "Failed"):
                break
        if etat == "Failed":
            raise RuntimeError(f"{nom} : échec de la synchronisation — {r.text}")
        r = client.get(f"{operation}/result")
    r.raise_for_status()
    echecs = [t for t in r.json()["value"] if t["status"] == "Failure"]
    if echecs:
        raise RuntimeError(f"{nom} : tables non synchronisées — {echecs}")
    print(f"{nom} : {len(r.json()['value'])} table(s) synchronisée(s)")

for nom in lakehouses.split(","):
    rafraichir(nom.strip())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
