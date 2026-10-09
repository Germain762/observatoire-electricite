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
| geo.api.gouv.fr | `/regions` | — | Base du référentiel régions |

---

## Architecture

```
                         BATCH (quotidien, pl_main)
API ODRÉ (cons-def) ─┐
                     ├─► Data Factory ─► lh_bronze ─► Spark ─► lh_silver ─► lh_gold ─► wh_electricite (étoile, sécurité)
API Open-Meteo ──────┘                                                                        │
                                                                                              └─► Power BI Direct Lake (étape 5)

                         STREAMING (toutes les 15 min)
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
| Git | Azure DevOps ; `elec-dev` synchronisé sur la branche `dev` ; `main` n'avance que par pull request depuis `dev` |
| Dossier Git | `/fabric` pour les items Fabric, la racine reste libre pour la doc et le code hors Fabric |
| Lakehouses | `lh_bronze`, `lh_silver`, `lh_gold`, `lh_admin` |
| Format des modèles sémantiques | Grands modèles sémantiques, identique sur les deux workspaces |
| Dossiers du workspace | Un dossier par couche (`bronze`, `silver`, `gold`, `realtime`, `warehouse`) |
| Calcul Spark | Starter pool ; délai d'expiration des sessions réduit à 10 min |

### Structure du repo

```
observatoire-electricite/
├─ README.md
├─ fabric/      ← items Fabric synchronisés
└─ docs/
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
| `cnx_` | Connexion |

Colonnes temporelles : le fuseau est toujours explicite dans le nom (`ts_utc`, `ts_paris`, `heure_utc`, `heure_paris`, `date_locale`).

---

## Orchestration batch

```
pl_main  (run_id = @pipeline().RunId, annee_debut : 0 = automatique)
├─ Set variable annee_debut_effective   (0 → année courante - 1)
├─ pl_orchestration_bronze
│   ├─ df_ref_regions
│   ├─ pl_ingest_eco2mix ┐ en parallèle
│   ├─ pl_ingest_meteo   ┘
│   └─ nb_check_bronze ─► alerte sur échec
├─ pl_orchestration_silver_gold  (run_id, annee_debut transmis)
│   ├─ nb_silver_eco2mix
│   ├─ nb_silver_meteo     (en séquence, pas en parallèle : voir « Capacité trial »)
│   └─ nb_gold
└─ pl_orchestration_warehouse    (run_id, annee_debut transmis)
    ├─ etl.charger_dim_date, etl.charger_dim_region
    └─ etl.charger_fact_production, etl.charger_fact_conso_meteo
```

- **Planification** : `pl_main` tous les jours à 05:00 (fuseau de Paris), la nuit pour éviter les conflits de capacité avec le développement interactif.
- **`annee_debut` automatique** : par défaut, recharge l'année précédente et l'année courante (les seules dont les données peuvent encore changer). Un rechargement complet se lance à la main avec `annee_debut = 2021`.
- `annee_debut` et `run_id` sont transmis jusqu'aux notebooks et procédures. Le `RunId` de `pl_main` est propagé explicitement, car un pipeline appelé par Invoke Pipeline a son propre `RunId`.
- Chaque activité Notebook et Procédure stockée a une **politique de retry** (2 tentatives, 120 s).

---

## Couche Bronze

### Items

| Item | Rôle |
|---|---|
| `df_ref_regions` | Référentiel `ref_regions` : code INSEE, libellé, coordonnées du chef-lieu, indicateur `perimetre_eco2mix` |
| `pl_ingest_eco2mix` | Export Parquet éCO2mix, une itération par année (paramètres `annee_debut`, `annee_fin`) |
| `pl_ingest_meteo` | Lookup régions → Filter sur `perimetre_eco2mix` → ForEach → Copy Open-Meteo |
| `pl_orchestration_bronze` | Dataflow → les deux ingestions en parallèle → notebook de contrôle → alerte sur échec |
| `nb_check_bronze` | Vérifie qu'aucune combinaison région / année ne manque ; lève une exception sinon |

### Choix

- **Idempotence par écrasement** : un fichier par année (`annee=YYYY/`), réécrit à chaque exécution. Relancer ne crée jamais de doublon.
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
- **`MERGE` idempotent** avec `row_hash` (SHA-256) : seules les lignes réellement modifiées sont mises à jour. Le hash remplace les nulls par une sentinelle avant concaténation, car `concat_ws` ignore les nulls.
- **Fidélité à la source** : une ligne publiée vide reste vide en silver ; si RTE la complète, le `row_hash` change et le `MERGE` la met à jour automatiquement.
- **Pas de partitionnement** : environ 1,2 million de lignes ; partitionner créerait des petits fichiers sans gain.
- **Chemins OneLake dynamiques** : construits depuis `notebookutils.runtime.context["currentWorkspaceName"]`, pour que le notebook fonctionne sans modification dans `elec-test`.

### `nb_silver_meteo` → `meteo_horaire`

- Lecture du JSON multiligne, aplatissement des tableaux parallèles par `arrays_zip` + `explode`.
- Code région récupéré depuis le dossier `region=XX` (découverte de partitions).
- Même stratégie de `MERGE` sur (région, `ts_utc`).

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
| `fact_production_horaire` | `FILIERES` ne contient aucune colonne interdite | Bloquant |
| | Clé unique (région, heure, filière) | Bloquant |
| | Conservation : Σ MWh gold = Σ filières silver × 0,5 sur la même tranche | Bloquant |
| | Heures avec `nb_pas ≠ 2` : au-delà de 24 couples (région, heure) | Bloquant |
| | Heures avec `nb_pas ≠ 2` : jusqu'à 24 couples | Avertissement |
| `fact_conso_meteo_horaire` | Table non vide ; pas de démultiplication par les jointures ; clé unique | Bloquant |
| | 0 null météo ; nombre de régions = régions `perimetre_eco2mix` | Bloquant |
| | Heures incomplètes : même règle à deux niveaux que la production | Bloquant / Avertissement |
| `agg_mix_journalier` | Clé unique ; parts sommant à 1 (tolérance 1e-9) ; conservation par jour local | Bloquant |
| | `nb_heures` = heures civiles du jour (23 / 24 / 25), plafonnées à 24, premier jour de la plage exclu | Bloquant |

Le contrôle des filières interdites vérifie **la liste elle-même** : c'est le seul qui aurait détecté le double comptage de l'éolien.

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

Colonnes : `run_id`, `notebook`, `table_cible`, `statut` (`succes`, `echec`, `avertissement`), `lignes` (`numOutputRows` lu dans l'historique Delta), `message`, `horodatage_utc`.

Le journal sert aussi de **référence externe** pour le chargement du Warehouse (voir plus bas).

`notebookutils.notebook.exit` n'est appelé **qu'une fois, dans la dernière cellule** : il termine immédiatement le notebook.

---

## Real-Time Intelligence

### Producteur : `nb_producer_eco2mix_tr`

Notebook **Python** (sans Spark), planifié toutes les 15 minutes dans Fabric.

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

- **Lecture cross-database** du gold via le SQL analytics endpoint (`lh_gold.dbo.fact_production_horaire`).
- **Transposition du `replaceWhere`** : `DELETE` de la tranche `annee >= @annee_debut` puis `INSERT ... SELECT`, dans une **transaction**.
- **`TRY / CATCH`** : `ROLLBACK` puis `THROW` pour relancer l'erreur, afin que le pipeline voie l'échec (même rôle que le `raise` de `publier`). Testé avec un `THROW` forcé : la table reste intacte.
- **Contrôle contre le journal** : le SQL analytics endpoint synchronise les métadonnées Delta de façon **asynchrone** ; juste après l'écriture par `nb_gold`, il peut encore voir l'ancienne version. Un contrôle de conservation Warehouse ↔ gold ne le détecterait pas (les deux côtés liraient la même version périmée). La procédure compare donc le nombre de lignes chargées au `lignes` journalisé par `nb_gold` pour ce `run_id`, et échoue si le journal n'a pas de ligne pour ce run ou si les comptes diffèrent. Le retry de l'activité couvre les retards ponctuels.
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

---

## Capacité trial : enseignements

- Une capacité trial n'a **ni file d'attente ni burst** pour Spark : un job sans cœurs disponibles est rejeté (`TooManyRequestsForCapacity`, HTTP 430) au lieu d'attendre.
- Parades appliquées : notebooks silver en **séquence**, **retry** sur les activités, **délai d'expiration des sessions** réduit à 10 min, `pl_main` planifié la nuit.
- L'admission optimiste admet un job sur son minimum de nœuds puis le laisse grossir.

---

## Décisions pour l'étape 5 (Power BI)

Une RLS SQL dans le Warehouse fait basculer un modèle **Direct Lake on SQL** en DirectQuery. Décision :

- Modèle sémantique en **Direct Lake on OneLake** sur les tables du Warehouse : lecture directe des fichiers Delta, jamais de bascule en DirectQuery.
- Ce mode **n'applique pas** la RLS SQL : la sécurité du rapport est assurée par une **RLS dans le modèle** (rôle DAX).
- Pour ne définir les droits qu'une fois, la table `securite.acces_region` est intégrée (masquée) au modèle ; le rôle DAX filtre `dim.region` avec `USERPRINCIPALNAME()` et la même logique `'*'`. Les droits vivent dans une table, appliqués par deux moteurs.
- La RLS SQL reste en place pour les accès SQL directs.

---

## Maintenance des tables (prévu)

- Pas d'`OPTIMIZE` dans les notebooks de chargement ; prévu : un `nb_maintenance` planifié (`OPTIMIZE` puis `VACUUM`) et l'*optimize write* sur les tables gold.
- À vérifier pour l'étape 5 : V-Order sur les tables lues en Direct Lake.
- KQL : fixer explicitement la **politique de rétention** de `eco2mix_tr_raw` (seule archive des valeurs temps réel publiées).

---

## Feuille de route

| Étape | Contenu | État |
|---|---|---|
| 1 | Socle, Git, ingestion bronze | ✅ |
| 2 | Médaillon Spark (silver, gold), contrôles, journalisation, orchestration `pl_main` | ✅ |
| 3 | Real-Time Intelligence : producteur, Eventstream, Eventhouse / KQL, Real-Time Dashboard, Activator | ✅ |
| 4 | Warehouse : modèle en étoile, procédures transactionnelles, contrôles, CLS / DDM / RLS | ✅ (exécution planifiée complète à confirmer) |
| 5 | Power BI : modèle sémantique Direct Lake on OneLake, RLS du modèle | ⏳ |
| 6 | Deployment pipeline dev → test, variable libraries, OneLake security, monitoring | ⏳ |

### Bonus non réalisés

- `nb_maintenance` et propriétés *optimize write*.
- Généraliser `publier` au silver en lui passant aussi la fonction d'écriture (`MERGE`).
- Expérience petits fichiers / `OPTIMIZE` / V-Order chronométrée.
- `notebookutils.notebook.runMultiple` ; mode haute concurrence pour les pipelines.
- Warehouse : comparaison de performance SQL endpoint / Warehouse, clonage de table, time travel ; jours fériés dans `dim.date`.
- Secret du producteur dans Azure Key Vault.

---

## Points d'attention

- Les **connexions** Fabric ne sont pas versionnées dans Git (référencées par ID) : à recréer ou paramétrer par environnement (étape 6).
- Git ne versionne que les **définitions** des items, pas les données. Les sources batch étant en open data, tout est reconstructible ; les valeurs temps réel archivées dans l'Eventhouse, elles, ne le sont plus après la purge de RTE.
- Les planifications (`pl_main`, producteur) ne doivent être actives que dans un seul workspace.
- Projet réalisé sur une capacité **trial** Fabric.
