# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3c2b7b8f-7fab-450d-a1bb-4904943c9bd2",
# META       "default_lakehouse_name": "lh_admin",
# META       "default_lakehouse_workspace_id": "ad90f025-9a04-41a2-b271-9b5881e5e6ad",
# META       "known_lakehouses": [
# META         {
# META           "id": "3c2b7b8f-7fab-450d-a1bb-4904943c9bd2"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

fenetre_heures = 3      # monter à 48 pour un rattrapage manuel

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

try:
    from azure.eventhub import EventHubProducerClient, EventData
except ImportError:
    %pip install azure-eventhub -q
    from azure.eventhub import EventHubProducerClient, EventData

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import json, logging, time
from datetime import datetime, timedelta, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-tr/records"

with open("/lakehouse/default/Files/secrets/es_eco2mix_tr.txt") as f:
    CONNECTION_STRING = f.read().strip()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True)
log = logging.getLogger("producer")

session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=4, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def lire_fenetre(heures: int) -> list[dict]:
    debut = (datetime.now(timezone.utc) - timedelta(hours=heures)).strftime("%Y-%m-%dT%H:%M:%SZ")
    lignes, offset = [], 0
    while True:
        r = session.get(URL, params={"where": f"date_heure >= '{debut}'",
                                     "order_by": "date_heure", "limit": 100, "offset": offset},
                        timeout=30)
        r.raise_for_status()
        page = r.json()["results"]
        lignes += page
        if len(page) < 100:
            return lignes
        offset += 100

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from azure.eventhub import EventHubProducerClient, EventData

def envoyer(lignes: list[dict]) -> None:
    fetched_at = datetime.now(timezone.utc).isoformat()   # le même horodatage pour toute l'exécution
    client = EventHubProducerClient.from_connection_string(CONNECTION_STRING)
    with client:
        lot = client.create_batch()
        for ligne in lignes:
            ligne["fetched_at_utc"] = fetched_at
            evt = EventData(json.dumps(ligne))
            try:
                lot.add(evt)
            except ValueError:            # lot plein : on l'envoie et on en ouvre un nouveau
                client.send_batch(lot)
                lot = client.create_batch()
                lot.add(evt)
        if len(lot) > 0:
            client.send_batch(lot)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

t0 = time.monotonic()
lignes = lire_fenetre(fenetre_heures)
if not lignes:
    raise RuntimeError(f"Aucune ligne lue sur les {fenetre_heures} dernières heures")
envoyer(lignes)
log.info("%d lignes envoyées en %.1f s", len(lignes), time.monotonic() - t0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
