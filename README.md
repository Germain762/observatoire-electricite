# Observatoire de l'électricité française — Microsoft Fabric

Projet de bout en bout sur Microsoft Fabric, construit pour pratiquer toutes les couches de la plateforme
(Data Factory, Lakehouse/Spark, Real-Time Intelligence, Warehouse, Power BI, CI/CD) dans le cadre de la
préparation à la certification **DP-700 – Fabric Data Engineer Associate**.

Le projet croise les données de production et de consommation électrique régionales de RTE (éCO2mix)
avec des données météo, pour analyser le mix énergétique, la part du thermique et le lien
consommation / température, en historique (batch) comme en quasi temps réel (streaming).

---

## Sources de données

| Source | Jeu / endpoint | Granularité | Remarques |
|---|---|---|---|
| RTE via ODRÉ (Opendatasoft) | `eco2mix-regional-cons-def` (consolidé et définitif) | Demi-heure, par région, depuis janvier 2021 | Quota de 50 000 appels API / mois → ingestion via l'endpoint **export** (un appel par année), jamais par pagination des records |
| RTE via ODRÉ | `eco2mix-regional-tr` (temps réel) | Quart d'heure, par région | Mis à jour une fois par heure ; valeurs récentes révisables ; données du mois M purgées à la mi-mois M+1 |
| Open-Meteo | API archive (`archive-api.open-meteo.com/v1/archive`) | Horaire | Température, vent, rayonnement ; quelques jours de retard → date de fin = J-6 |
| Open-Meteo | API Previous Runs (`previous-runs-api.open-meteo.com/v1/forecast`) | Horaire | Prévisions **émises la veille** (`*_previous_day1`), archivées depuis 2024 ; utilisées pour le volet ML |
| geo.api.gouv.fr | `/regions` | — | Base du référentiel régions |

---

## Architecture

```
                         BATCH (quotidien, pl_main)
API ODRÉ (cons-def) ─┐
                     ├─► Data Factory ─► lh_bronze ─► Spark ─► lh_silver ─► lh_gold ─► wh_electricite (étoile, sécurité)
API Open-Meteo ──────┘                                                                        │
                                                                     sm_electricite (Direct Lake on OneLake) ◄─┘
                                                                              └─► rpt_electricite
                         MACHINE LEARNING
lh_gold + météo prévue J-1 ─► nb_ml_entrainement ─► exp_conso (MLflow) ─► ml_conso (modèle enregistré)
                                                                              │
                              nb_ml_scoring (PREDICT) ─► lh_gold.fact_prevision_conso ─► sm_electricite

                         STREAMING (toutes les heures)
API ODRÉ (tr) ─► nb_producer_eco2mix_tr ─► es_eco2mix_tr ─► eh_electricite (KQL) ─► rtd_electricite
                  (notebook Python)          (Eventstream)                          └─► act_thermique (alerte)

Journalisation des exécutions ─► lh_admin
```

Architecture médaillon (batch) :

- **Bronze** : fichiers bruts tels que livrés par les API (Parquet éCO2mix, JSON météo), plus le référentiel régions.
- **Silver** : données typées, dédoublonnées, clé en UTC, chargées par `MERGE` idempotent.
- **Gold** : tables analytiques au grain horaire et journalier, protégées par des contrôles bloquants avant écriture.
- **Warehouse** : couche de service — modèle en étoile, contrôles de chargement, sécurité SQL. Les transformations restent dans Spark.
- **Admin** : tables techniques (journal des exécutions, secret du producteur), séparées des données métier.

Le streaming suit la même logique en trois couches dans l'Eventhouse : table brute → table typée (update policy) → vue dédoublonnée (materialized view).

---

## Environnements et organisation

| Élément | Choix |
|---|---|
| Workspaces | `elec-dev` (développement), `elec-test` (cible du deployment pipeline) |
| Déploiement | Deployment pipeline `dp_electricite` (Développement → Test) |
| Paramétrage par environnement | Variable library `vl_electricite`, jeux de valeurs `jv_dev` et `jv_test` |
| Git | Azure DevOps ; `elec-dev` synchronisé sur la branche `dev` ; `main` n'avance que par pull request depuis `dev` |
| Dossier Git | `/fabric` pour les items Fabric, la racine reste libre pour la doc et le code hors Fabric |
| Lakehouses | `lh_bronze`, `lh_silver`, `lh_gold`, `lh_admin` |
| Format des modèles sémantiques | Grands modèles sémantiques, identique sur les deux workspaces |
| Dossiers du workspace | Un dossier par couche (`bronze`, `silver`, `gold`, `realtime`, `warehouse`, `powerbi`, `ml`) |
| Calcul Spark | Starter pool ; délai d'expiration des sessions réduit à 10 min |

### Structure du repo

```
observatoire-electricite/
├─ README.md
├─ fabric/      ← items Fabric synchronisés
└─ docs/
   └─ audit_deploiement.md   ← audit de portabilité dev → test
```

### Conventions de nommage

| Préfixe | Type d'item |
|---|---|
| `lh_` | Lakehouse |
| `wh_` | Warehouse |
| `pl_` | Pipeline |
| `df_` | Dataflow Gen2 |
| `nb_` | Notebook |
| `es_` | Eventstream |
| `eh_` | Eventhouse |
| `kqs_` | KQL Queryset |
| `rtd_` | Real-Time Dashboard |
| `act_` | Activator |
| `exp_` | Expérience MLflow |
| `ml_` | Modèle ML enregistré |
| `sm_` | Modèle sémantique |
| `rpt_` | Rapport Power BI |
| `vl_` | Variable library |
| `dp_` | Deployment pipeline |
| `cnx_` | Connexion |

Colonnes temporelles : le fuseau est toujours explicite dans le nom (`ts_utc`, `ts_paris`, `heure_utc`, `heure_paris`, `date_locale`).

---

## Orchestration batch

```
pl_main  (run_id = @pipeline().RunId, annee_debut : 0 = automatique)
├─ Set variable annee_debut_effective   (0 → année courante - 1)
├─ pl_orchestration_bronze       (run_id, annee_debut transmis)
│   ├─ df_ref_regions
│   ├─ pl_ingest_eco2mix          ┐ en parallèle
│   ├─ ForEach annees → pl_ingest_meteo (une exécution par année) ┘
│   └─ nb_check_bronze ─► alerte sur échec
├─ pl_orchestration_silver_gold  (run_id, annee_debut transmis)
│   ├─ nb_silver_eco2mix
│   ├─ nb_silver_meteo     (en séquence ; session partagée avec nb_silver_eco2mix, session tag « silver »)
│   └─ nb_gold
└─ pl_orchestration_warehouse    (run_id, annee_debut transmis)
    ├─ nb_refresh_sql_endpoints   (synchronisation forcée des SQL endpoints de lh_gold et lh_admin)
    ├─ etl.initialiser_environnement   (référentiels statiques, accès ETL, droits ; idempotente)
    ├─ etl.charger_dim_date, etl.charger_dim_region
    ├─ etl.charger_fact_production, etl.charger_fact_conso_meteo
    └─ Actualisation de sm_electricite (reframing explicite)
```

- **Planification** : `pl_main` tous les jours à 05:00 (fuseau de Paris), la nuit pour éviter les conflits de capacité avec le développement interactif.
- **`annee_debut` automatique** : par défaut, recharge l'année précédente et l'année courante (les seules dont les données peuvent encore changer). Un rechargement complet se lance à la main avec `annee_debut = 2021`.
- `annee_debut` et `run_id` sont transmis jusqu'aux notebooks et procédures. Le `RunId` de `pl_main` est propagé explicitement, car un pipeline appelé par Invoke Pipeline a son propre `RunId`.
- Chaque activité Notebook et Procédure stockée a une **politique de retry** (2 tentatives, 120 s). Un retry absorbe une erreur **passagère** ; un échec à chaque première tentative est un bug qu'il rend invisible (voir l'incident du 19/10). Le taux d'échec au premier essai se surveille dans `ItemJobEventLogs`.
- **Haute concurrence pour les pipelines** : `nb_silver_eco2mix` et `nb_silver_meteo` partagent une session Spark (même session tag, même lakehouse par défaut `lh_silver`, même utilisateur, même configuration). `nb_gold`, dont le lakehouse par défaut est `lh_gold`, ne peut pas la rejoindre.

---

## Couche Bronze

### Items

| Item | Rôle |
|---|---|
| `df_ref_regions` | Référentiel `ref_regions` : code INSEE, libellé, coordonnées du chef-lieu, indicateur `perimetre_eco2mix` |
| `pl_ingest_eco2mix` | Export Parquet éCO2mix, une itération par année (paramètres `annee_debut`, `annee_fin`) |
| `pl_ingest_meteo` | Paramètre `annee` : Lookup régions → Filter sur `perimetre_eco2mix` → ForEach régions (séquentiel) → Copy Open-Meteo de l'année vers `meteo/archive/region=XX/annee=YYYY/` → attente de 5 s |
| `pl_orchestration_bronze` | Dataflow → éCO2mix et boucle séquentielle sur les années météo en parallèle → notebook de contrôle → alerte sur échec |
| `nb_check_bronze` | Vérifie qu'aucune combinaison région / année ne manque ; lève une exception sinon |

### Choix

- **Idempotence par écrasement** : un fichier par année (`annee=YYYY/`), réécrit à chaque exécution. Relancer ne crée jamais de doublon.
- **Météo incrémentale, sur le modèle d'éCO2mix** : un fichier par région **et par année**, et seules les années ≥ `annee_debut` sont retéléchargées (2 ans par nuit au lieu de près de 6). La boucle sur les années vit dans `pl_orchestration_bronze` et celle sur les régions dans `pl_ingest_meteo` (pas de ForEach imbriqué). L'année de fin est celle de J-6 : entre le 1er et le 6 janvier, la nouvelle année n'a pas encore de données dans l'archive. Requêtes d'un an au lieu de six : l'attente entre deux appels passe de 20 s à 5 s.
- **Nouvelle racine `meteo/archive/`** : faire cohabiter l'ancienne structure (`region=XX/meteo.json`) et la nouvelle (`region=XX/annee=YYYY/`) sous la même racine aurait fait échouer la découverte de partitions de Spark. Les anciens dossiers ont été supprimés après migration.
- **Copie en Binary** : le Parquet de l'API est recopié tel quel, sans parsing dans le pipeline.
- **Pas de ForEach imbriqué** (non supporté dans les pipelines Fabric) : découpage en pipelines enfants appelés par Invoke Pipeline.
- **Corse hors périmètre** : la Corse n'est pas raccordée au réseau continental et n'apparaît pas dans éCO2mix régional. Elle reste dans le référentiel avec `perimetre_eco2mix = false` ; chaque traitement filtre selon ses besoins, rien n'est exclu en dur.
- **Contrôle qui échoue bruyamment** : le notebook de contrôle lève une exception, le pipeline voit l'échec et déclenche l'alerte.

---

## Couche Silver

### `nb_silver_eco2mix` → `eco2mix_regional`

- **Clé** : (`code_insee_region`, `ts_utc`). L'UTC est la seule référence temporelle fiable ; `ts_paris` n'est qu'un attribut (à l'heure locale, la nuit du passage à l'heure d'hiver contient deux fois 2h-3h).
- **Session Spark en UTC** : `spark.sql.session.timeZone = UTC`.
- **Dédoublonnage déterministe** : `row_number` sur (région, `date_heure`), en gardant d'abord la ligne dont le libellé `heure` correspond à l'heure locale recalculée depuis `date_heure`, puis le définitif avant le consolidé.
- **Comptage avant / après dédoublonnage** journalisé. Valeur attendue : 24 lignes supprimées par mois de mars présent dans la plage.
- **Nullification du détail éolien avant 2024** (voir qualité des données).
- **`MERGE` de synchronisation de la tranche** avec `row_hash` (SHA-256) : seules les lignes réellement modifiées sont mises à jour. Le hash remplace les nulls par une sentinelle avant concaténation, car `concat_ws` ignore les nulls.
- **Propagation des suppressions** : `whenNotMatchedBySourceDelete(condition = "t.annee >= annee_debut")`. Une ligne retirée par RTE disparaît du silver ; la condition limite la suppression à la tranche rechargée (sans elle, une exécution avec `annee_debut = 2025` supprimerait 2021-2024).
- **Garde-fou** : les lignes à supprimer sont comptées **avant** le `MERGE` ; au-delà de 500, l'exécution échoue (export probablement incomplet) ; sinon, elles sont journalisées en avertissement après le `MERGE`. Un seul `MERGE` par exécution.
- **Fidélité à la source** : une ligne publiée vide reste vide en silver ; si RTE la complète, le `row_hash` change et le `MERGE` la met à jour automatiquement.
- **Pas de partitionnement** : environ 1,2 million de lignes ; partitionner créerait des petits fichiers sans gain.
- **Chemins OneLake dynamiques** : construits depuis `notebookutils.runtime.context["currentWorkspaceName"]`, pour que le notebook fonctionne sans modification dans `elec-test`.

### `nb_silver_meteo` → `meteo_horaire`

- Lecture du JSON multiligne, aplatissement des tableaux parallèles par `arrays_zip` + `explode`.
- Code région et année récupérés depuis les dossiers `region=XX` et `annee=YYYY` (découverte de partitions).
- **Élagage de partitions** : filtre `annee >= annee_debut` à la lecture, seuls les fichiers récents sont lus.
- Même stratégie de `MERGE` sur (région, `ts_utc`), avec une borne `t.ts_utc >= annee_debut-01-01` pour que Delta ignore les fichiers anciens de la cible. Pas de suppression des lignes absentes de la source : l'archive Open-Meteo ne retire pas d'heures.
- **Migration validée** : rechargement complet (`annee_debut = 2021`) dans la nouvelle structure, nombre de lignes de `meteo_horaire` inchangé et aucune insertion dans le `MERGE`.

---

## Couche Gold

### Tables

| Table | Grain | Colonnes |
|---|---|---|
| `fact_production_horaire` | région × heure UTC × filière | `code_region`, `heure_utc`, `heure_paris`, `filiere`, `mwh`, `nb_pas`, `annee` |
| `fact_conso_meteo_horaire` | région × heure UTC | `code_region`, `heure_utc`, `heure_paris`, `conso_mwh`, `nb_pas`, `temperature_c`, `vent_kmh`, `rayonnement_wm2`, `annee` |
| `agg_mix_journalier` | région × jour local × filière | `code_region`, `date_locale`, `filiere`, `mwh`, `mwh_total_jour`, `part`, `nb_heures`, `annee` |

### Choix communs

- **Grain horaire** : nécessaire pour joindre la météo (horaire) et suffisant pour l'analyse.
- **MW → MWh** : éCO2mix publie une puissance (MW). Sur un pas demi-heure, énergie = MW × 0,5 h ; agrégation par `date_trunc('hour', ts_utc)`.
- **`nb_pas`** : nombre de demi-heures non nulles dans l'heure (2 attendu).
- **Pas d'imputation** : une demi-heure absente ou vide à la source n'est pas estimée ; l'heure garde l'énergie mesurée avec `nb_pas = 1`.
- **`heure_paris`** : l'heure locale est calculée dans Spark (`from_utc_timestamp`), pour que la règle de conversion de fuseau n'existe qu'à un seul endroit. Le Warehouse la reprend sans reconvertir.
- **Évolution de schéma** : l'ajout de `heure_paris` a demandé `mergeSchema` dans `write_slice`, puis un rechargement complet (`annee_debut = 2021`) pour remplir la colonne sur tout l'historique.
- **Écriture par `replaceWhere`** sur `annee >= annee_debut` : le gold est intégralement recalculable depuis le silver, la tranche concernée est remplacée de façon atomique. `MERGE` en silver, `replaceWhere` en gold.
- **Coût du `replaceWhere` sans partitionnement** : observé avec `annee_debut = 2025`, Delta a retiré les 9 fichiers et recopié les 2,9 millions de lignes 2021-2024 (`numCopiedRows`) pour remplacer 1,1 million de lignes. Négligeable sur 22 Mo.

### `fact_production_horaire`

- Format long obtenu par `unpivot`, puis filtre `mw IS NOT NULL`.
- La constante `FILIERES` définit une **partition** de la production : thermique, nucléaire, éolien (total), solaire, hydraulique, bioénergies, déstockage batterie.
- Exclus : consommation, pompage, stockage batterie, échanges physiques (pas de la production), taux `tco_*` / `tch_*` (des pourcentages, pas des MW), détail éolien terrestre / offshore (double comptage avec le total).

### `fact_conso_meteo_horaire`

- Consommation agrégée à l'heure, puis jointures **left** vers la météo : une heure sans météo reste visible avec des nulls.
- **Alignement temporel** : température et vent (valeurs instantanées) joints sur l'heure `h` ; rayonnement (moyenne sur l'heure précédente chez Open-Meteo) joint sur `h + 1 h`, par jointure sur l'horodatage décalé et non par `lead`.

### `agg_mix_journalier`

- Lue depuis `fact_production_horaire` (déjà contrôlée).
- **Jour local** : `date_locale = to_date(from_utc_timestamp(heure_utc, 'Europe/Paris'))`. Le filtre d'entrée et `annee` portent sur `date_locale`.
- `mwh_total_jour` calculé par fenêtre, `part = mwh / mwh_total_jour`.
- **Une part n'est pas additive** : `mwh` et `mwh_total_jour` sont conservés pour recalculer une part sur n'importe quelle période.

---

## Contrôles et journalisation (batch)

### Principe

- Les contrôles s'exécutent **avant l'écriture** : une donnée fausse n'atteint jamais le gold.
- Ils vérifient des **invariants**, jamais des valeurs codées en dur.
- Les échecs sont **accumulés** puis levés ensemble (`raise ValueError`), avec le nombre de cas et quelques exemples.
- Deux niveaux : **bloquant** (le pipeline échoue) et **avertissement** (journalisé, le pipeline continue).

### Contrôles par table

| Table | Contrôle | Niveau |
|---|---|---|
| `eco2mix_regional` (silver) | Lignes absentes de la source rechargée : au-delà de 500 | Bloquant |
| | Lignes absentes de la source rechargée : jusqu'à 500 (supprimées) | Avertissement |
| `fact_production_horaire` | `FILIERES` ne contient aucune colonne interdite | Bloquant |
| | Clé unique (région, heure, filière) | Bloquant |
| | Conservation : Σ MWh gold = Σ filières silver × 0,5 sur la même tranche | Bloquant |
| | Heures avec `nb_pas ≠ 2` : au-delà de 24 couples (région, heure) | Bloquant |
| | Heures avec `nb_pas ≠ 2` : jusqu'à 24 couples | Avertissement |
| `fact_conso_meteo_horaire` | Table non vide ; pas de démultiplication par les jointures ; clé unique | Bloquant |
| | 0 null météo ; nombre de régions = régions `perimetre_eco2mix` | Bloquant |
| | Heures incomplètes : même règle à deux niveaux que la production | Bloquant / Avertissement |
| `agg_mix_journalier` | Clé unique ; parts sommant à 1 (tolérance 1e-9) ; conservation par jour local | Bloquant |
| | Journées dont `nb_heures` ≠ heures civiles du jour (23 / 24 / 25, plafonnées à 24, premier jour de la plage exclu) : au-delà de 3 couples (région, jour) | Bloquant |
| | Mêmes journées incomplètes : jusqu'à 3 couples | Avertissement |

Le contrôle des filières interdites vérifie **la liste elle-même** : c'est le seul qui aurait détecté le double comptage de l'éolien.

Le contrôle journalier couvre un angle mort du contrôle `nb_pas` : une heure entièrement vide ou absente du silver disparaît de `fact_production_horaire` (filtre `mw IS NOT NULL`) au lieu d'y apparaître avec `nb_pas = 1` ; seul le décompte des heures par jour local la voit.

### Fonctions partagées (`nb_utils`, inclus via `%run nb_utils`)

| Fonction | Rôle |
|---|---|
| `row_hash(cols)` | SHA-256 des colonnes avec sentinelle pour les nulls |
| `write_slice(df, table, annee_debut)` | Création de la table au premier passage, puis `replaceWhere` (avec `mergeSchema`) |
| `log_execution(table, statut, lignes, message)` | Ajout d'une ligne au journal |
| `lever_si_echecs(erreurs, table)` | Lève une erreur regroupant tous les contrôles en échec |
| `publier(df, table, controles, annee_debut)` | Cache → contrôles → écriture → log succès ; log échec puis `raise` en cas d'erreur ; libération du cache dans `finally` |

`publier` reçoit la fonction de contrôle **en paramètre** (l'équivalent d'un délégué `Func<DataFrame, List<string>>` en C#) et l'appelle dans son `try`.

### Journal des exécutions : `lh_admin.dbo.log_execution`

Colonnes : `run_id`, `environnement` (`dev` / `test`, lu dans `vl_electricite`), `notebook`, `table_cible`, `statut` (`succes`, `echec`, `avertissement`), `lignes` (`numOutputRows` lu dans l'historique Delta), `message`, `horodatage_utc`.

Le journal sert aussi de **référence externe** pour le chargement du Warehouse (voir plus bas).

`notebookutils.notebook.exit` n'est appelé **qu'une fois, dans la dernière cellule** : il termine immédiatement le notebook.

---

## Real-Time Intelligence

### Producteur : `nb_producer_eco2mix_tr`

Notebook **Python** (sans Spark), planifié **toutes les heures** dans Fabric : la source n'est mise à jour qu'une fois par heure, une fréquence plus élevée n'apporterait rien. Avec une fenêtre de 3 heures, chaque quart d'heure est envoyé environ 3 fois.

- **Sans état, fenêtre glissante** : chaque passage relit les `fenetre_heures` dernières heures (3 par défaut) et envoie tout. Une valeur révisée par RTE est donc toujours récupérée, et une exécution ratée est rattrapée par la suivante.
- **Livraison au moins une fois** côté producteur, **déduplication idempotente** côté consommateur (materialized view `arg_max`).
- **Rattrapage** : lancement manuel avec un `fenetre_heures` plus grand. Plafond d'environ 200 h, car l'endpoint `records` d'Opendatasoft ne pagine pas au-delà de 10 000 lignes (48 lignes par heure). Les données temps réel restant disponibles jusqu'à la mi-mois suivant, un rattrapage initial de 192 h a été fait pour disposer d'un historique.
- **Envoi en plusieurs lots** : un lot Event Hubs est limité en taille (environ 1 Mo) ; un nouveau lot est ouvert dès que le précédent est plein.
- **`fetched_at_utc`** : même horodatage pour toutes les lignes d'une exécution ; sert à départager les versions et à mesurer les délais.
- **Robustesse** : retries HTTP (`HTTPAdapter` + `Retry`, backoff, codes 429 et 5xx), journalisation par `logging`, échec explicite si la fenêtre ne renvoie aucune ligne.
- **Secret** : la chaîne de connexion de l'Eventstream est stockée dans `lh_admin` (zone Files), jamais dans le notebook ni dans Git. Compromis assumé pour un projet personnel (pas de rotation, pas d'audit) ; la cible serait un Azure Key Vault lu par `notebookutils.credentials.getSecret`.
- La planification ne doit **pas** être active dans `elec-test`.

### Eventstream : `es_eco2mix_tr`

- **Source Custom endpoint** (mode push, protocole Event Hub) : le producteur envoie avec le SDK standard `azure-eventhub`.
- **Destination Eventhouse en ingestion directe** vers `eco2mix_tr_raw` : pas de transformation dans le flux, le typage se fait en KQL (même principe que le bronze).

### Eventhouse : `eh_electricite`

| Objet | Rôle |
|---|---|
| `eco2mix_tr_raw` | Table brute, telle qu'envoyée par le producteur ; archive des valeurs temps réel publiées par RTE |
| `f_eco2mix_tr_typage()` | Fonction de typage (noms de colonnes, `todatetime`, `toreal`) |
| `eco2mix_tr` | Table typée, alimentée par **update policy** depuis la table brute |
| `mv_eco2mix_tr_dedup` | **Materialized view** `arg_max(fetched_at_utc, *) by code_region, ts_utc` : dernière version de chaque quart d'heure |
| `f_dernier_etat()` | Dernier quart d'heure renseigné par région |
| `f_mix_national(debut, fin)` | Mix et part thermique nationale par quart d'heure, uniquement sur les quarts d'heure complets (toutes les régions renseignées) |

- **Update policy non transactionnelle** (`IsTransactional = false`) : un échec de transformation ne fait jamais perdre la donnée brute ; il se consulte avec `.show ingestion failures`.
- L'update policy ne traite que les **nouvelles** ingestions : l'existant a été repris une fois par `.set-or-append`, les éventuels doublons étant absorbés par la materialized view.
- `mv_eco2mix_tr_dedup` interrogée directement renvoie la partie matérialisée plus le delta récent ; `materialized_view()` ne renvoie que la partie matérialisée.

### Analyses KQL (`kqs_eco2mix_tr`)

- **Délai de publication** (premier `fetched_at_utc` non nul − `ts_utc`) : _résultats p50 / p95 à compléter_.
- **Révisions** (`dcount(tostring(consommation))` par quart d'heure, null compté comme une version) : _distribution à compléter_.
- **Anomalies de consommation** : `make-series` au pas de 15 min, `series_fill_linear`, `series_decompose_anomalies` (seuil 2,5).

### Restitution et alerte

- **`rtd_electricite`** : part thermique nationale, mix national en aires empilées, dernier état par région, consommation et anomalies de la région choisie (paramètre `_region`) ; actualisation automatique toutes les 15 minutes. Les tuiles appellent des **fonctions stockées** : la logique est versionnée avec la base KQL et réutilisée par l'alerte.
- **`act_thermique`** : alerte Activator sur la part thermique nationale, seuil calé sur le p95 observé, déclenchement au franchissement du seuil. Testée en abaissant temporairement le seuil.

---

## Warehouse : `wh_electricite`

### Structure

| Schéma | Contenu |
|---|---|
| `dim` | `dim.date`, `dim.region`, `dim.filiere` |
| `fact` | `fact.production_horaire`, `fact.conso_meteo_horaire` |
| `etl` | Procédures de chargement |
| `securite` | Table des droits, fonction de prédicat, stratégie de sécurité |

- **Types** : `datetime2(n)` au lieu de `datetime`, `varchar` (UTF-8) au lieu de `nvarchar`, non supportés dans Fabric Warehouse.
- **Contraintes non appliquées** : clés primaires `NONCLUSTERED NOT ENFORCED`. Elles informent l'optimiseur mais n'empêchent aucun doublon ; l'unicité reste vérifiée par des requêtes de contrôle.
- **Clés naturelles** : `date_key` au format `AAAAMMJJ`, code région, code filière.
- **Structure créée une fois par DDL**, versionnée avec le Warehouse ; les procédures ne font que remplir.

### Dimensions

- **`dim.date`** : générée par une table de nombres (`VALUES` × `CROSS JOIN`), du 01/01/2021 au 31/12/2027 (2 556 jours). Jour de la semaine calculé par `DATEDIFF(day, '1900-01-01', d) % 7 + 1` (le 1er janvier 1900 était un lundi), indépendamment de `DATEFIRST`. Libellés français par `CASE`.
- **`dim.region`** : depuis `lh_bronze.dbo.ref_regions`, filtrée sur `perimetre_eco2mix`, avec une colonne fictive `contact_email` pour pratiquer le masquage dynamique.
- **`dim.filiere`** : table statique avec les attributs renouvelable, bas carbone et pilotable. Le déstockage batterie est classé non renouvelable et non bas carbone par convention (une batterie restitue de l'énergie stockée, ce n'est pas une source).

### Chargement des faits

- **Initialisation de l'environnement** : `etl.initialiser_environnement`, idempotente, remplit ce que le projet SQL versionne sans contenu — les valeurs de `dim.filiere`, la ligne `'*'` de `securite.acces_region` pour l'identité de chargement (indispensable avant tout `DELETE` filtré par la RLS) et les `GRANT` du rôle `lecteur_regional`. Un déploiement copie les tables et le rôle, pas leurs lignes ni leurs droits.
- **Lecture cross-database** du gold via le SQL analytics endpoint (`lh_gold.dbo.fact_production_horaire`).
- **Transposition du `replaceWhere`** : `DELETE` de la tranche `annee >= @annee_debut` puis `INSERT ... SELECT`, dans une **transaction**.
- **`TRY / CATCH`** : `ROLLBACK` puis `THROW` pour relancer l'erreur, afin que le pipeline voie l'échec (même rôle que le `raise` de `publier`). Testé avec un `THROW` forcé : la table reste intacte.
- **Le retard du SQL endpoint** : le SQL analytics endpoint d'un lakehouse synchronise les métadonnées Delta de façon **asynchrone** ; juste après l'écriture par `nb_gold`, il peut encore voir l'ancienne version des tables. La procédure lirait alors des données périmées sans aucune erreur. Deux protections indépendantes :
  - **Synchronisation forcée** : `nb_refresh_sql_endpoints` (notebook Python, première activité de `pl_orchestration_warehouse`) appelle l'API REST `POST /v1/workspaces/{id}/sqlEndpoints/{id}/refreshMetadata` pour `lh_gold` et `lh_admin`, attend la fin de l'opération longue (`202` + `Location`) et échoue si une table n'est pas synchronisée.
  - **Contrôle contre le journal** : un contrôle de conservation Warehouse ↔ gold ne détecterait pas le retard (les deux côtés liraient la même version périmée). La procédure compare donc le nombre de lignes chargées au `lignes` journalisé par `nb_gold` pour ce `run_id`, et échoue si le journal n'a pas de ligne pour ce run ou si les comptes diffèrent.
- **Cohérence vérifiée** : une requête en étoile (faits × `dim.date` × `dim.filiere`) donne les mêmes totaux annuels que le gold.

### Sécurité

- **Rôle** `lecteur_regional` avec `SELECT` sur les schémas `dim` et `fact`, aucun droit sur `securite`.
- **CLS** : `SELECT` accordé sur une liste de colonnes de `dim.region`, sans `contact_email`.
- **DDM** : `contact_email` masqué avec `email()`.
- **RLS** :
  - Table des droits `securite.acces_region (utilisateur, code_region)`, `'*'` pour toutes les régions ; l'utilisateur est identifié par `USER_NAME()` (UPN).
  - Fonction de prédicat inline `securite.fn_filtre_region(@code_region)` avec `SCHEMABINDING`.
  - Stratégie `securite.politique_region` : prédicats de **filtre** sur les deux tables de faits et `dim.region` (Fabric Warehouse ne supporte pas les prédicats de blocage).
  - Les lecteurs n'ont pas besoin de droits sur la table des droits (chaînage des propriétés).
- **Constat** : la RLS s'applique aussi à l'**administrateur du workspace** (vérifié en restreignant temporairement son propre accès à la région 28).
- **Conséquence pour l'ETL** : un prédicat de filtre s'applique aussi aux `DELETE`. L'identité qui exécute `pl_main` doit avoir l'accès `'*'`, sinon le `DELETE` ne supprimerait que les lignes visibles avant de tout réinsérer (doublons silencieux).

---

## Qualité des données : constats et règles

| Constat | Analyse | Règle appliquée |
|---|---|---|
| `tch_nucleaire` null dans 5 régions | Régions sans centrale nucléaire : capacité installée nulle, taux de charge indéfini | Null conservé : il a un sens métier |
| Détail éolien terrestre + offshore = 0 de 2021 à 2023 | Ventilation non publiée avant 2024, remplie par des **faux zéros** | Détail nullifié avant le 01/01/2024 en silver |
| Écarts de 1 MW entre `eolien` et le détail depuis 2024 | Signature d'un arrondi séparé des composantes | Tolérance documentée, `eolien` fait foi |
| Écarts de plus de 1 MW (max 20 MW), détail toujours supérieur au total | Uniquement quand l'offshore vaut 0 ; hypothèse : consommation propre des parcs en mer à l'arrêt | `eolien` fait foi ; le détail reste en silver |
| Passage à l'heure d'été (mars) : 2 doublons par région | La source publie toujours 48 créneaux locaux ; les libellés 02:00 / 02:30 inexistants tombent sur le même instant UTC que 03:00 / 03:30 | Dédoublonnage déterministe sur la cohérence de l'heure locale |
| Passage à l'heure d'hiver (octobre) : 1 heure manquante par région et par an | Grille fixe de 48 créneaux pour une journée de 25 h | Perte à la source documentée, non imputée |
| Ligne vide le 31/12/2025 à 23:00 UTC (minuit à Paris le 01/01/2026), 12 régions | Artefact de publication à la jonction définitif 2025 / consolidé 2026 | Conservée en silver ; heure gold à `nb_pas = 1` ; avertissement journalisé |
| Temps réel : quarts d'heure annoncés vides puis remplis, valeurs révisées | Publication horaire, données provisoires | Fenêtre glissante + `arg_max` ; quarts d'heure incomplets exclus des calculs nationaux |
| 6 demi-heures du 16/02/2025 (région 84) retirées de la publication | Le bronze de dev et celui de test en contiennent 42 ; le silver de dev en gardait 48, car le `MERGE` ne supprimait jamais | Suppression des lignes absentes de la source dans la tranche rechargée (plafond 500) ; journée à 21 h signalée en avertissement |

Leçon générale : **une ligne présente n'est pas une donnée présente** ; un null peut avoir un sens métier, et un zéro peut mentir.

---

## Analyses de validation

### Alignement du rayonnement

| Décalage | -1 h | 0 | **+1 h** | +2 h |
|---|---|---|---|---|
| Corrélation moyenne (par région et année) | 0,850 | 0,937 | **0,942** | 0,864 |

Pic à +1 h, conforme à la documentation d'Open-Meteo ; sommet estimé vers +0,56 h (incertitude résiduelle d'environ 30 min). Calculée globalement, la même corrélation ne donne que 0,67 : agréger des groupes hétérogènes masque une relation très forte à l'intérieur de chaque groupe.

### Consommation / température

Corrélation négative dans toutes les régions (chauffage électrique), de -0,71 (Normandie) à -0,41 (PACA). Les régions méditerranéennes, plus faibles, ont vraisemblablement une relation non linéaire (climatisation l'été).

### Production annuelle (12 régions)

| Année | Gold |
|---|---|
| 2021 | ~520 TWh |
| 2022 | ~443 TWh (indisponibilité d'une partie du parc nucléaire) |
| 2023 | ~493 TWh |

Légèrement inférieures aux bilans nationaux de RTE (écart correspondant à la Corse). Ce contrôle a révélé le double comptage de l'éolien en 2024 avant sa correction.

---

## Tests de validation

| Test | Attendu |
|---|---|
| Relance complète de l'orchestration bronze | Mêmes fichiers, aucun doublon |
| `DESCRIBE HISTORY eco2mix_regional` après une deuxième exécution | 0 ligne insérée, 0 mise à jour |
| `replaceWhere` avec `annee_debut = 2021` puis `2025` | Les deux exécutions passent, nombre de lignes inchangé |
| Contrôles gold | `eolien_terrestre` ajouté à `FILIERES` → échec de `nb_gold` et ligne `echec` dans `log_execution` |
| `pl_main` de bout en bout | Une ligne `succes` par table gold avec le même `run_id`, Warehouse chargé |
| Producteur temps réel | Arrivées régulières dans `eco2mix_tr_raw` sur 24 h ; aucun échec d'ingestion sur `eco2mix_tr` |
| Materialized view | `IsHealthy = true` ; aucune paire (région, quart d'heure) en double |
| Alerte | `act_thermique` déclenchée avec un seuil abaissé, e-mail reçu |
| `dim.date` | 2 556 jours, autant de clés distinctes ; le 01/01/2021 est un vendredi |
| Procédure de faits avec `THROW` forcé | `ROLLBACK`, table inchangée |
| Requête en étoile | Mêmes totaux annuels que le gold |
| RLS | Accès restreint à `'28'` → seule la Normandie visible ; accès `'*'` rétabli ensuite |
| `nb_refresh_sql_endpoints` | Statut `Success` pour toutes les tables de `lh_gold` et `lh_admin` |
| Relance isolée de `pl_orchestration_warehouse` | Avec `annee_debut` et le `run_id` d'une exécution passée, le chargement passe sans relancer bronze, silver ni gold |
| `MERGE` silver | `DESCRIBE HISTORY eco2mix_regional` : une seule opération `MERGE` par exécution |
| Déploiement dev → test | Un correctif fait en dev arrive dans `elec-test` par `dp_electricite`, sans retouche manuelle ; `pl_main` au vert dans les deux workspaces |
| Isolation des environnements | Après actualisation de `df_ref_regions` dans `elec-test`, l'historique de `ref_regions` de dev ne montre aucune écriture ; `log_execution` de test porte `environnement = test` |
| Mode du modèle sémantique | TMDL : connecteur « Azure Data Lake Storage » (Direct Lake on OneLake) |
| RLS du modèle | « Tester en tant que rôle » avec l'utilisateur `'28'` → seule la Normandie, sur toutes les pages |
| Reframing | `pl_main` se termine par l'actualisation de `sm_electricite`, visible dans l'historique d'actualisation |
| Cohérence du rapport | Totaux annuels identiques à ceux du gold |
| Performances | Analyseur de performances : aucune requête DirectQuery |
| Échecs au premier essai | `ItemJobEventLogs` : aucun `Failed` pour les notebooks de `pl_main` (les retries ne doivent plus servir) |
| Migration météo | Rechargement complet dans `meteo/archive/` : nombre de lignes de `meteo_horaire` inchangé, 0 insertion dans le `MERGE` |
| Encodage de la région (ML) | Modèle réentraîné sur 2021-2024 avec `region_id` : même score sur 2025 (~140 MWh) qu'avec la catégorie pandas |
| Chaîne de scoring (ML) | `fact_prevision_conso` écrite par PREDICT, cohérente avec les prédictions du notebook |
| OneLake security | Utilisateur de test membre de `lecteur_normandie` : seule la région 28 via le SQL endpoint de `lh_gold` ; rien sur `lh_silver` (refus par défaut) |

---

## Incidents d'exploitation

| Date | Symptôme | Cause | Détection | Correction |
|---|---|---|---|---|
| 07/10/2026 | `nb_gold` refusé par Spark (`TooManyRequestsForCapacity`, HTTP 430) | Capacité trial sans file d'attente ; notebooks silver lancés en parallèle | Échec de l'activité dans `pl_main` | Silver en séquence, retries, expiration des sessions à 10 min, planification la nuit |
| 07/10/2026 | `fact_production_horaire` bloquée : « heures incomplètes » | Ligne vide publiée par RTE à la jonction définitif 2025 / consolidé 2026 | Contrôle `nb_pas` | Règle à deux niveaux (avertissement / bloquant) |
| 10/10/2026 | `etl.charger_fact_production` en échec : « Aucune écriture gold journalisée pour ce run » | SQL endpoint de `lh_admin` / `lh_gold` pas encore synchronisé à 05:00, malgré 2 retries de 120 s | Contrôle contre `log_execution` (transaction annulée, Warehouse intact) | `run_id` vérifiés identiques sur toute la chaîne ; rattrapage par relance de `pl_orchestration_warehouse` seul avec le `run_id` du matin ; ajout de `nb_refresh_sql_endpoints` |

| 13/10/2026 | Premier `pl_main` dans `elec-test` : `agg_mix_journalier` bloque (région 84, 16/02/2025, 21 h au lieu de 24) | 6 demi-heures retirées par RTE ; le silver de dev les conservait (`MERGE` sans suppression), le test chargé à neuf ne les avait plus | Contrôle journalier de `agg_mix_journalier` ; comparaison des bronzes dev / test | Suppression des lignes absentes de la source dans la tranche, avec garde-fou ; contrôle journalier à deux niveaux ; correctif déployé dev → test par `dp_electricite` |

| 19/10/2026 | Aucun symptôme visible : `pl_main` au vert, mais environ 6 min d'attentes de retry par nuit, révélées par le monitoring | `nb_silver_eco2mix`, `nb_gold` et `nb_refresh_sql_endpoints` refusés à **chaque** première tentative (`TooManyRequestsForCapacity`) depuis le retour au starter pool ; les retries de 120 s rattrapaient l'erreur | `ItemJobEventLogs` : 10 exécutions dont 5 échecs pour chacun, contre 0 échec pour `pl_main` | Haute concurrence pour les pipelines (session partagée entre les deux silver) ; si insuffisant, environnement à nœuds Medium plafonnés à 2-3 nœuds |

Sans le contrôle contre le journal, l'incident du 10/10 aurait rechargé silencieusement les données de la veille avec un pipeline au vert. L'incident du 13/10 n'aurait jamais été visible en dev seul : c'est le chargement à neuf d'un second environnement qui a révélé des lignes fantômes accumulées depuis le début du projet.

---

## Monitoring

### Qui voit quoi

| Outil | Ce qu'il voit | Ce qu'il ne voit pas |
|---|---|---|
| Workspace monitoring (`ItemJobEventLogs`, `EventhouseQueryLogs`…, en préversion) | Exécutions de pipelines, Dataflows et notebooks planifiés ou lancés par un pipeline ; requêtes de l'Eventhouse | Les messages d'erreur ; ce que les traitements ont fait |
| Monitoring hub | Toutes les exécutions, avec le détail et l'erreur | Pas de requête ni d'historique long |
| Capacity Metrics app | La consommation (CU) par élément et par opération ; le détail d'un instant donné | Le métier |
| `log_execution` | Contrôles, avertissements, lignes écrites, environnement | Durées, capacité |

`JobInstanceId` de `pl_main` = `run_id` de `log_execution` : le monitoring dit quand et combien de temps, le journal dit ce qui s'est passé.

### Durées observées (`ItemJobEventLogs`, état final de chaque exécution par `arg_max`)

| Élément | p50 | p95 |
|---|---|---|
| `pl_main` | 37,4 min | 45,9 min |
| `pl_orchestration_bronze` | 18,9 min | 26,4 min |
| dont `pl_ingest_meteo` | 15,5 min | 23,2 min |
| dont `pl_ingest_eco2mix` | 3,5 min | 6,8 min |
| `pl_orchestration_silver_gold` | 12,1 min | 13,2 min |
| `pl_orchestration_warehouse` | 4,8 min | 5,2 min |
| `nb_producer_eco2mix_tr` | 0,4 min | 0,5 min |

Ces durées sont antérieures aux corrections du 19-20/10. La météo était le chemin critique du bronze (exécutée en parallèle d'éCO2mix, quatre fois plus lente) : `date_debut = 2021-01-01` était transmise en dur, et près de 6 ans d'historique horaire étaient retéléchargés chaque nuit pour 12 régions, avec une attente de 20 s par région. Corrigé par l'ingestion météo incrémentale (voir la couche Bronze) ; les retries de l'incident du 19/10 ont été traités par la haute concurrence.

### Consommation de capacité (Capacity Metrics app, éléments principaux)

| Bloc | CU (s) | Part |
|---|---|---|
| Monitoring (Eventhouse, Eventstream et Activator de monitoring) | 515 198 | 42 % |
| Temps réel (`es_eco2mix_tr`, `eh_electricite`) | 386 376 | 32 % |
| Batch dev (`nb_gold`, `pl_ingest_*`, `wh_electricite`, `nb_silver_eco2mix`, `nb_check_bronze`, `df_ref_regions`) | 299 183 | 25 % |
| Test | 15 648 | 1 % |

Utilisation moyenne de 35 %, aucun rejet sur la période.

### Enseignements

- **Le monitoring coûte plus que ce qu'il surveille** : 42 % de la consommation, atteints en environ 4 jours d'activation. À activer en connaissance de cause et à limiter dans le temps sur une capacité payante.
- **Un Eventhouse est facturé à son temps d'activité** : l'Eventhouse de monitoring, alimenté en continu, ne se met jamais en veille ; `eh_electricite`, alimenté une fois par heure, n'a été actif qu'environ 34 h sur la période.
- **Un Eventstream coûte surtout par sa durée de fonctionnement** : pour un petit lot horaire, une ingestion directe dans l'Eventhouse depuis le notebook serait plus économique en production.
- **Le producteur en notebook Python est quasi gratuit** : il n'apparaît pas parmi les éléments principaux.
- **Un endpoint Eventhouse activé par erreur sur `wh_electricite`** représentait 97 % des exécutions de jobs (5 717 synchronisations de schéma en 4 jours, environ une par minute). Supprimé : il n'était utilisé par rien.
- **Une absence d'événement ne produit aucune ligne** : pour détecter les heures sans exécution du producteur, `make-series ... default = 0` plutôt qu'un `summarize` par heure.

### Alerte prévue

Une règle Activator sur `ItemJobEventLogs | where ItemName == "pl_main" and JobStatus =~ "Failed"`, avec le `JobInstanceId` dans le message pour retrouver la cause dans `log_execution`. Non mise en place.

---

## Capacité trial : enseignements

- Une capacité trial n'a **ni file d'attente ni burst** pour Spark : un job sans cœurs disponibles est rejeté (`TooManyRequestsForCapacity`, HTTP 430) au lieu d'attendre.
- Parades appliquées : notebooks silver en **séquence**, **retry** sur les activités, **délai d'expiration des sessions** réduit à 10 min, `pl_main` planifié la nuit.
- L'admission optimiste admet un job sur son minimum de nœuds puis le laisse grossir.
- Un pool à petits nœuds avait fait disparaître les erreurs 430 mais doublait la durée du pipeline ; le retour au starter pool les a fait revenir, masquées par les retries (incident du 19/10).
- Un **notebook Python** est soumis à la même limite : `nb_refresh_sql_endpoints`, sans Spark, a été refusé avec le même message.

---

## Power BI

### Choix du mode : Direct Lake on OneLake

| | Direct Lake on SQL | Direct Lake on OneLake (retenu) |
|---|---|---|
| Lecture des données | Via le SQL endpoint du Warehouse | Directement les fichiers Delta dans OneLake |
| Sécurité SQL du Warehouse (RLS, CLS, DDM) | Appliquée, mais bascule en DirectQuery | **Ignorée** |
| Bascule en DirectQuery | Oui | Jamais |

Une RLS SQL dans le Warehouse ferait basculer un modèle Direct Lake on SQL en DirectQuery. Le mode OneLake garde les performances de Direct Lake, mais n'applique aucune sécurité SQL : la sécurité du rapport est donc assurée **dans le modèle**. Le mode se vérifie dans le TMDL (nœud Expressions) : connecteur « Azure Data Lake Storage ».

### Modèle sémantique `sm_electricite`

- **Créé depuis Power BI Desktop** (catalogue OneLake → `wh_electricite`), édité en direct dans le workspace.
- **Tables** : Date, Région, Filière, Production, Consommation, Accès région (masquée). Noms lisibles, clés techniques masquées.
- **`contact_email` exclu du modèle** : le masquage dynamique du Warehouse ne s'appliquant pas en mode OneLake, la colonne aurait été lisible en clair. Ce qui ne doit pas être vu n'entre pas dans le modèle.
- **Relations** plusieurs-à-un, filtre dans un seul sens, des faits vers Date, Région et Filière ; aucune relation sur Accès région.
- **Table de dates** : `dim.date` marquée comme table de dates ; libellés de mois et de jours triés par leur numéro. Aucune colonne calculée (en préversion dans ce mode) : les libellés viennent du Warehouse.

### Mesures

| Mesure | Rôle |
|---|---|
| `Production MWh`, `Production TWh` | Sommes de base |
| `Part filière` | Rapport des sommes avec `REMOVEFILTERS('Filière')` au dénominateur : une part se calcule en mesure, jamais en colonne agrégée |
| `Part bas carbone` | Part des filières `bas_carbone` dans la production |
| `Consommation MWh`, `Taux de couverture` | Production / consommation : au-dessus de 1, la région exporte vers ses voisines |
| `Production N-1`, `Évolution N/N-1` | Intelligence temporelle sur la table de dates |
| `Heures incomplètes` | Heures avec `nb_pas < 2` : la qualité de données est visible dans le rapport lui-même |

### Sécurité du modèle

- **Rôle `lecteur_regional`** avec deux filtres DAX :
  - sur Région : codes autorisés pour `USERPRINCIPALNAME()` lus dans Accès région, avec la même logique `'*'` que la fonction de prédicat SQL ; le filtre se propage aux faits par les relations ;
  - sur Accès région : `[utilisateur] = USERPRINCIPALNAME()`, pour qu'un lecteur ne voie pas les droits des autres (masquer une table ne la protège pas).
- **Une seule source de droits** : la table `securite.acces_region`, appliquée par deux moteurs (fonction de prédicat SQL pour les accès SQL directs, rôle DAX pour Power BI). Ajouter un lecteur reste un simple `INSERT`.
- **Constat** : dans un modèle sémantique, la RLS ne s'applique qu'aux lecteurs ; administrateurs, membres et contributeurs du workspace n'y sont pas soumis. C'est l'inverse de la RLS du Warehouse, qui s'applique aussi à l'administrateur.
- **Test sans deuxième compte** : « Tester en tant que rôle » dans le service, avec l'UPN de la ligne `'28'` → seule la Normandie apparaît, sur toutes les pages.
- **Connexion à identité fixe** recommandée par la documentation pour une RLS sur un modèle Direct Lake : le modèle lit OneLake avec une identité unique plutôt qu'avec celle de chaque lecteur.

### Reframing explicite

Un modèle Direct Lake pointe sur une **version** des tables Delta ; le **reframing** le fait pointer vers la dernière. La mise à jour automatique est désactivée et le reframing est déclenché par une activité d'actualisation du modèle **à la fin** de `pl_orchestration_warehouse`. Les procédures chargeant les tables l'une après l'autre, un reframing automatique aurait pu montrer la production du jour à côté de la consommation de la veille. Le rapport passe ainsi d'un état cohérent à un autre, en une seule fois — le même principe que les transactions SQL, appliqué au modèle.

### Rapport `rpt_electricite`

| Page | Contenu |
|---|---|
| Vue nationale | Production mensuelle par filière, part bas carbone, évolution N/N-1, lien vers `rtd_electricite` |
| Régions | Matrice région × production, consommation, taux de couverture, part bas carbone |
| Consommation et température | Nuage de points des moyennes journalières, segment par région |
| Qualité | `Heures incomplètes` dans le temps |

Construit avec l'Analyseur de performances ouvert : toutes les requêtes en Direct Lake, aucune en DirectQuery.

Le modèle (TMDL) et le rapport (PBIR) sont versionnés dans Git comme les autres items, avec un diff lisible.

---

## Déploiement et environnements

### Principe

`elec-test` est alimenté par le deployment pipeline `dp_electricite` depuis `elec-dev`. Un déploiement copie les **définitions** des éléments, jamais les données, les permissions, les membres des rôles d'un modèle sémantique ni les identifiants de connexion. `elec-test` a donc été chargé à neuf par son propre `pl_main` (`annee_debut = 2025`).

L'audit de portabilité qui a précédé le premier déploiement est dans [`docs/audit_deploiement.md`](docs/audit_deploiement.md).

### Ce qui est portable par construction

- **Références relatives** : dans les définitions versionnées, Fabric désigne les éléments du même workspace par leur ID **logique** et un `workspaceId` à zéro (« workspace courant ») ; pipelines, activités Notebook, Copy, Eventstream et Queryset se résolvent ainsi dans l'environnement de destination.
- **Références par nom** : `lh_silver.dbo…`, `lh_admin.dbo.log_execution`, requêtes cross-database du Warehouse — toutes relatives au workspace d'exécution.
- **Notebooks** : reconnexion automatique au lakehouse par défaut du workspace de destination ; chemins OneLake construits depuis le nom du workspace courant.

### Variable library `vl_electricite`

| Variable | Consommateur | Rôle |
|---|---|---|
| `environnement` | `nb_utils` (`notebookutils.variableLibrary`) | Colonne `environnement` de `log_execution` |
| `workspace_id`, `lh_bronze_id` | `df_ref_regions` (`Variable.Value` dans `mashup.pq`) | Destination du Dataflow dans le bon `lh_bronze` |
| `workspace_id`, `sm_electricite_id` | `pl_orchestration_warehouse` (`@pipeline().libraryVariables`) | Actualisation du bon modèle sémantique |
| `alertes_actives`, `email_alertes` | — | Prévues pour les alertes ; désactivées en test |

- Jeux `jv_dev` et `jv_test`, le jeu actif étant défini par étape du deployment pipeline.
- `Variable.Value` **sans valeur par défaut** dans le Dataflow : une valeur par défaut serait forcément un ID de dev, qui ferait écrire en silence dans le mauvais workspace ; sans défaut, l'actualisation échoue bruyamment.
- La destination d'un Dataflow ne se modifie pas dans l'interface : `mashup.pq` est édité dans Git, puis « Tout mettre à jour » dans le workspace.

### Règles de déploiement

- `sm_electricite` : **règle de source de données** vers `wh_electricite` de test. Un modèle Direct Lake ne se reconnecte pas tout seul au déploiement : sans règle, le modèle de test lirait le Warehouse de dev.

### Étapes manuelles après un premier déploiement

1. Désactiver dans `elec-test` les planifications de `pl_main` et de `nb_producer_eco2mix_tr` (elles sont versionnées avec `enabled = true`), ainsi que l'Activator.
2. Renseigner le jeu `jv_test` avec les ID des éléments de test, l'activer pour l'étape Test, puis redéployer.
3. Exécuter `etl.initialiser_environnement` (ou laisser `pl_orchestration_warehouse` le faire) avant tout chargement.
4. Reconfigurer `sm_electricite` : membres du rôle `lecteur_regional`, connexion à identité fixe.
5. Exclure du déploiement l'élément de workspace monitoring, propre à chaque workspace.

### Enseignements

- **Toute modification manuelle dans une étape aval est écrasée au déploiement suivant.** La destination du Dataflow, corrigée à la main dans `elec-test`, serait repartie vers le bronze de dev au déploiement suivant ; seule la variable library rend la correction durable.
- **Un second environnement chargé à neuf est un test en soi** : il a révélé les lignes fantômes du silver de dev (incident du 13/10).
- **Ce qui est versionné n'est pas ce qui est nécessaire** : le projet SQL versionne les tables et le rôle, pas les lignes de référence ni les `GRANT` — d'où la procédure d'initialisation idempotente.

### Les couches de sécurité du projet

| Couche | Ce qu'elle contrôle | Qui y est soumis | Constat dans le projet |
|---|---|---|---|
| Rôles du workspace | Accès aux éléments et au contenu du workspace | Tous les utilisateurs du workspace | Administrateur, membre et contributeur ont un accès élevé ; les autres couches s'appliquent surtout aux lecteurs |
| Permissions d'élément | Partage d'un élément précis (lecture, données, génération) | Utilisateurs hors rôles du workspace | — |
| OneLake security | Rôles de lecture sur un lakehouse, avec RLS / CLS par table ; refus par défaut | Lecteurs (administrateurs, membres et contributeurs ne sont pas filtrés) | Rôle `lecteur_normandie` sur `lh_gold` (Read sur les trois tables, RLS `code_region = '28'`) ; SQL endpoint de `lh_gold` passé en mode identité de l'utilisateur ; utilisateur de test retiré de `DefaultReader` |

**OneLake security : points clés**

- Le rôle `DefaultReader` contient virtuellement tous les détenteurs de ReadAll : un utilisateur ajouté à un rôle restreint mais resté dans `DefaultReader` garde l'accès complet.
- En mode **identité déléguée** (par défaut), le SQL endpoint ignore la OneLake security ; il faut le passer en **mode identité de l'utilisateur**. L'ETL n'est pas affecté (identité d'administrateur, non filtrée).
- RLS et CLS d'une même table doivent être dans le même rôle ; le nom de table d'une règle doit correspondre exactement, sinon elle ne renvoie aucune ligne, sans erreur.
- Un modèle Direct Lake on OneLake n'applique la OneLake security que s'il utilise l'**identité de l'utilisateur** ; `sm_electricite`, en identité fixe et lisant le Warehouse, n'en tient pas compte, d'où la RLS du modèle. Une architecture « tout par l'identité de l'utilisateur » (OneLake security + Direct Lake on OneLake en SSO) centraliserait les droits en un seul endroit.
| Sécurité SQL du Warehouse | RLS (`politique_region`), CLS (`GRANT` par colonne), masquage dynamique | Requêtes SQL sur le Warehouse | La RLS s'applique **aussi à l'administrateur** du workspace (vérifié) ; filtre aussi les `DELETE` de l'ETL |
| RLS du modèle sémantique | Rôle DAX `lecteur_regional` | **Uniquement les lecteurs** du modèle | Testée avec « Tester en tant que rôle » ; ignore la sécurité SQL en Direct Lake on OneLake |

---

## Machine learning : prévision de la consommation

Volet de découverte du ML dans Fabric : prévoir la consommation horaire de chaque région à partir du calendrier, de la météo et de la consommation de la semaine précédente.

### Démarche

| Étape | Choix | Pourquoi |
|---|---|---|
| Problème | Régression supervisée, cible `conso_mwh` horaire par région | La valeur à prédire est un nombre, les exemples passés donnent la bonne réponse |
| Référence | « Même heure, une semaine avant » (`conso_j7`) | Un modèle qui ne bat pas une règle simple ne sert à rien |
| Découpage | Entraînement 2021-2023, validation 2024, test 2025 regardé une seule fois | Un découpage aléatoire laisserait voir le futur ; décider sur le test le rend optimiste |
| Mesures | MAE (MWh) et MAPE (%) | La MAE est dominée par les grandes régions, la MAPE donne le même poids à chacune |
| Suivi | Expérience MLflow `exp_conso` | Chaque essai garde ses paramètres, ses scores et son modèle |

### Variables explicatives

| Variable | Rôle |
|---|---|
| `heure`, `jour_semaine`, `mois` (heure de Paris) | Rythmes de la vie : la journée, la semaine, les saisons |
| `ferie`, `pont`, `ferie_j7` | Jours fériés (bibliothèque `holidays`), ponts, et semaine précédente fériée (sinon `conso_j7` trompe le modèle) |
| `conso_j7` | Mémoire : niveau de la région et tendance récente |
| `temperature_c`, `chauffage = max(0, 15 − T)`, `climatisation = max(0, T − 22)`, vent, rayonnement | Météo ; le rayonnement avec le même décalage d'une heure que dans le gold |
| `temp_moy_24h`, `temp_moy_72h` | Inertie thermique : un bâtiment refroidi plusieurs jours continue de chauffer |
| `delta_temp_j7` | Écart de température avec la semaine précédente : quand `conso_j7` n'est plus représentatif |

`conso_j7` et `temp_j7` sont obtenus par **jointure sur l'horodatage décalé** et les moyennes par `rolling("24h")` sur une **durée** : un `shift` ou une fenêtre en nombre de lignes se décaleraient sur les heures manquantes, sans rien signaler.

### Résultats (test 2025)

| Modèle | MAE (MWh) | MAPE |
|---|---|---|
| Référence naïve J-7 | 336 | 7,8 % |
| Régression linéaire | 280 | 7,1 % |
| Régression linéaire, heures / jours / mois en colonnes 0/1 | 262 | 6,8 % |
| LightGBM | 172 | 4,2 % |
| LightGBM + jours fériés | 166 | 4,0 % |
| LightGBM + jours fériés + inertie, météo **observée** | **140** | **3,45 %** |
| Même modèle, météo **prévue la veille** (Previous Runs) | **155** | **3,72 %** |

- **La performance réaliste est la dernière ligne** : la veille au soir, on ne dispose que de la météo prévue. Plus de deux fois moins d'erreur que la référence. Toutes les heures utilisent la prévision, y compris celles des moyennes glissantes qui seraient déjà observées : le chiffre est plutôt pessimiste.
- **La prévision météo n'explique qu'environ 10 % de l'erreur** (+15 MWh pour une erreur moyenne de prévision de température de 1,1 °C) : le reste vient du modèle.
- Les scores antérieurs aux variables d'inertie ont été mesurés sur 2025 en décidant au vu du test : ils sont légèrement optimistes. Le score de 140 / 155 est le seul obtenu avec le découpage en trois périodes.

### Enseignements

- **Pourquoi les arbres battent la régression linéaire** : la régression additionne des effets indépendants. Elle ne sait représenter ni les **interactions** (froid × 19h × jour ouvré), ni une **échelle** propre à chaque région : un seul coefficient en MWh par degré, dominé par les grandes régions, fait exploser l'erreur relative des petites (Bourgogne-Franche-Comté et Centre-Val de Loire à 11 %, contre 5 % pour PACA). Les arbres de LightGBM découpent par seuils et apprennent ces combinaisons seuls.
- **Des variables redondantes rapportent peu** : passer les heures en colonnes 0/1 n'améliore la régression que de 6 %, car `conso_j7` contient déjà le profil de la journée.
- **L'inertie thermique est l'amélioration la plus rentable** (172 → 140 MWh). Elle a corrigé la vague de froid du 17-23 novembre 2025, où le modèle restait ancré sur une semaine précédente douce (+50 % de consommation d'une semaine à l'autre).
- **Un modèle n'apprend bien que ce qu'il a vu souvent** : les ponts de mai 2025, le 24 décembre ou le début août restent les pires jours. Ce sont des limites connues, non corrigées pour ne pas réutiliser le test.
- **Les arbres n'extrapolent pas** : au-delà des valeurs vues à l'entraînement, LightGBM répond avec sa feuille la plus proche. Hypothèse écartée pour la vague de froid (températures rares mais déjà vues), à garder en tête.

### Mise en production

| Élément | Rôle |
|---|---|
| `lh_silver.dbo.meteo_prevue_j1` | Prévisions de la veille d'Open-Meteo, collectées une fois (requêtes mensuelles, avec retries) |
| `nb_ml_utils` | Calcul des variables, partagé par l'entraînement et le scoring (`%run`) |
| `nb_ml_entrainement` | Vérification de l'encodage, puis entraînement du modèle de production sur 2021-2025 et enregistrement |
| `exp_conso` | Expérience MLflow : tous les essais et leurs scores |
| `ml_conso` | Modèle enregistré (version 1), au format MLflow avec sa signature |
| `nb_ml_scoring` | Scoring par lots avec PREDICT (`MLFlowTransformer`) |
| `lh_gold.dbo.fact_prevision_conso` | Prévision et réel par région et par heure, avec `source_meteo` et `modele` pour la traçabilité |

- **Un seul code de calcul des variables** pour l'entraînement et le scoring : sinon, une partie de l'écart observé viendrait d'un calcul différent et non du modèle (*training-serving skew*).
- **Entrées simples et stables** : PREDICT reçoit un DataFrame Spark, où le type « category » de pandas n'existe pas. La région devient un numéro `region_id` défini par une liste figée dans le code (et non déduite des données), déclarée en `categorical_feature` à LightGBM ; toutes les variables sont converties en décimaux pour une signature sans ambiguïté. Un contrôle vérifie que ce changement d'encodage ne dégrade pas le score.
- **Modèle de production entraîné sur toutes les données** une fois l'évaluation faite : les scores calculés ensuite sur 2025 ne sont plus que des contrôles de la chaîne, pas des mesures de performance.
- **Rapport** : `fact_prevision_conso` ajoutée à `sm_electricite` (Direct Lake on OneLake combine des tables du Warehouse et de `lh_gold`), reliée à Date et à Région — la RLS du modèle filtre donc aussi les prévisions. Page « réel contre prévision » avec les mesures `Prévision MWh`, `Erreur absolue MWh` et `MAPE prévision`.

### Pour une vraie prévision quotidienne de J+1

Les données consolidées de RTE arrivent avec environ un mois de retard : `conso_j7` devrait venir du **flux temps réel** (`eh_electricite`), et la météo du lendemain de l'API de prévision d'Open-Meteo. Le scoring deviendrait une étape nocturne de `pl_main`, où se rejoindraient les parties batch et temps réel du projet.

---

## Maintenance des tables (prévu)

- Pas d'`OPTIMIZE` dans les notebooks de chargement ; prévu : un `nb_maintenance` planifié (`OPTIMIZE` puis `VACUUM`) et l'*optimize write* sur les tables gold.
- À vérifier : V-Order sur les tables lues en Direct Lake.
- KQL : fixer explicitement la **politique de rétention** de `eco2mix_tr_raw` (seule archive des valeurs temps réel publiées).

---

## Feuille de route

| Étape | Contenu | État |
|---|---|---|
| 1 | Socle, Git, ingestion bronze | ✅ |
| 2 | Médaillon Spark (silver, gold), contrôles, journalisation, orchestration `pl_main` | ✅ |
| 3 | Real-Time Intelligence : producteur, Eventstream, Eventhouse / KQL, Real-Time Dashboard, Activator | ✅ |
| 4 | Warehouse : modèle en étoile, procédures transactionnelles, contrôles, synchronisation forcée des SQL endpoints, CLS / DDM / RLS | ✅ |
| 5 | Power BI : modèle sémantique Direct Lake on OneLake, mesures, RLS du modèle, reframing explicite, rapport | ✅ |
| 6 | Deployment pipeline dev → test, variable library, audit de portabilité, initialisation d'environnement, monitoring, OneLake security | ✅ |
| 7 | Machine learning : prévision de la consommation (LightGBM, MLflow, modèle enregistré, PREDICT, page de rapport) ; ingestion météo incrémentale | ✅ |

### À confirmer

- Haute concurrence : plus aucun échec au premier essai dans `ItemJobEventLogs` après l'exécution du 20/10.
- OneLake security : test avec l'utilisateur de test (région 28 uniquement).
- Monitoring : baisse de la consommation de l'Eventhouse de monitoring après la suppression de l'endpoint Eventhouse.

### Bonus non réalisés

- `nb_maintenance` et propriétés *optimize write*.
- Généraliser `publier` au silver en lui passant aussi la fonction d'écriture (`MERGE`).
- Expérience petits fichiers / `OPTIMIZE` / V-Order chronométrée.
- `notebookutils.notebook.runMultiple` ; mode haute concurrence pour les pipelines.
- Warehouse : comparaison de performance SQL endpoint / Warehouse, clonage de table, time travel ; jours fériés dans `dim.date`.
- Secret du producteur dans Azure Key Vault.
- Alerte Activator sur l'échec de `pl_main` à partir de `ItemJobEventLogs`.
- Ingestion directe dans l'Eventhouse depuis le producteur, sans Eventstream, et comparaison de la consommation.
- ML : prévision quotidienne de J+1 (consommation temps réel + API de prévision Open-Meteo, scoring dans `pl_main`) ; vacances scolaires comme variable ; réglage des paramètres de LightGBM sur le jeu de validation.

---

## Points d'attention

- Les **connexions** Fabric ne sont pas versionnées dans Git (référencées par ID). Celles des API publiques (`cnx_odre`, `cnx_openmeteo`) sont partagées par les deux workspaces ; les références à des éléments par ID physique passent par `vl_electricite` ou une règle de déploiement.
- Git ne versionne que les **définitions** des items, pas les données. Les sources batch étant en open data, tout est reconstructible ; les valeurs temps réel archivées dans l'Eventhouse, elles, ne le sont plus après la purge de RTE.
- Les planifications (`pl_main`, producteur) ne doivent être actives que dans un seul workspace.
- Projet réalisé sur une capacité **trial** Fabric.
