# Observatoire de l'électricité française — Microsoft Fabric

Projet de bout en bout sur Microsoft Fabric, construit pour pratiquer toutes les couches de la plateforme
(Data Factory, Lakehouse/Spark, Warehouse, Real-Time Intelligence, Power BI, CI/CD) dans le cadre de la
préparation à la certification **DP-700 – Fabric Data Engineer Associate**.

Le projet croise les données de production et de consommation électrique régionales de RTE (éCO2mix)
avec des données météo, pour analyser le mix énergétique, la part du thermique et le lien
consommation / température.

---

## Sources de données

| Source | Jeu / endpoint | Granularité | Remarques |
|---|---|---|---|
| RTE via ODRÉ (Opendatasoft) | `eco2mix-regional-cons-def` (consolidé et définitif) | Demi-heure, par région, depuis janvier 2021 | Quota de 50 000 appels API / mois → ingestion via l'endpoint **export** (un appel par année), jamais par pagination des records |
| RTE via ODRÉ | `eco2mix-regional-tr` / `eco2mix-national-tr` (temps réel) | Quart d'heure | Réservé à la partie Real-Time (étape 3) |
| Open-Meteo | API archive (`archive-api.open-meteo.com/v1/archive`) | Horaire | Température, vent, rayonnement ; quelques jours de retard → date de fin = J-6 |
| geo.api.gouv.fr | `/regions` | — | Base du référentiel régions |

---

## Architecture

```
API ODRÉ ─┐
          ├─► Data Factory ─► lh_bronze (Files, brut) ─► Notebooks Spark ─► lh_silver (Delta) ─► lh_gold (Delta)
API Météo ┘                                                                                        │
                                                                                                   ├─► Warehouse (étape 4)
API temps réel ─► producteur Python ─► Eventstream ─► Eventhouse / KQL (étape 3)                   └─► Power BI Direct Lake (étape 5)

Journalisation des exécutions ─► lh_admin
```

Architecture médaillon :

- **Bronze** : fichiers bruts tels que livrés par les API (Parquet éCO2mix, JSON météo), plus le référentiel régions.
- **Silver** : données typées, dédoublonnées, clé en UTC, chargées par `MERGE` idempotent.
- **Gold** : tables analytiques au grain horaire et journalier, protégées par des contrôles bloquants avant écriture.
- **Admin** : tables techniques (journal des exécutions), séparées des données métier lues par Power BI.

---

## Environnements et organisation

| Élément | Choix |
|---|---|
| Workspaces | `elec-dev` (développement), `elec-test` (cible du deployment pipeline) |
| Git | Azure DevOps ; `elec-dev` synchronisé sur la branche `dev` ; `main` n'avance que par pull request depuis `dev` |
| Dossier Git | `/fabric` pour les items Fabric, la racine reste libre pour la doc et le code hors Fabric |
| Lakehouses | `lh_bronze`, `lh_silver`, `lh_gold`, `lh_admin` |
| Format des modèles sémantiques | Grands modèles sémantiques, identique sur les deux workspaces |
| Dossiers du workspace | Un dossier par couche (`bronze`, `silver`, `gold`) |
| Calcul Spark | Starter pool ; délai d'expiration des sessions réduit à 10 min |

### Structure du repo

```
observatoire-electricite/
├─ README.md
├─ fabric/      ← items Fabric synchronisés
├─ producer/    ← producteur Python temps réel (étape 3)
└─ docs/
```

### Conventions de nommage

| Préfixe | Type d'item |
|---|---|
| `lh_` | Lakehouse |
| `pl_` | Pipeline |
| `df_` | Dataflow Gen2 |
| `nb_` | Notebook |
| `cnx_` | Connexion |

Colonnes temporelles : le fuseau est toujours explicite dans le nom (`ts_utc`, `ts_paris`, `heure_utc`, `date_locale`).

---

## Orchestration

```
pl_main  (run_id = @pipeline().RunId, annee_debut)
├─ pl_orchestration_bronze
│   ├─ df_ref_regions
│   ├─ pl_ingest_eco2mix ┐ en parallèle
│   ├─ pl_ingest_meteo   ┘
│   └─ nb_check_bronze ─► alerte sur échec
└─ pl_orchestration_silver_gold  (run_id, annee_debut transmis)
    ├─ nb_silver_eco2mix
    ├─ nb_silver_meteo     (en séquence, pas en parallèle : voir « Capacité trial »)
    └─ nb_gold
```

- `annee_debut` et `run_id` sont transmis jusqu'aux notebooks comme **paramètres de base** (cellule de paramètres).
- Le `RunId` de `pl_main` est propagé explicitement : un pipeline appelé par Invoke Pipeline a son propre `RunId`, et toutes les lignes de journal d'une même exécution doivent partager le même identifiant.
- Chaque activité Notebook a une **politique de retry** (2 tentatives, 120 s).

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
- **Incrémental** : relancer avec `annee_debut` = année précédente, car les données consolidées deviennent définitives en cours d'année A+1.
- **Copie en Binary** : le Parquet de l'API est recopié tel quel, sans parsing dans le pipeline.
- **Pas de ForEach imbriqué** (non supporté dans les pipelines Fabric) : découpage en pipelines enfants appelés par Invoke Pipeline.
- **Corse hors périmètre** : la Corse n'est pas raccordée au réseau continental et n'apparaît pas dans éCO2mix régional. Elle reste dans le référentiel avec `perimetre_eco2mix = false` ; chaque traitement filtre selon ses besoins, rien n'est exclu en dur.
- **Contrôle qui échoue bruyamment** : le notebook de contrôle lève une exception, le pipeline voit l'échec et déclenche l'alerte. Test : passer la Corse à `true` doit faire échouer le contrôle.

---

## Couche Silver

### `nb_silver_eco2mix` → `eco2mix_regional`

- **Clé** : (`code_insee_region`, `ts_utc`). L'UTC est la seule référence temporelle fiable ; `ts_paris` n'est qu'un attribut (à l'heure locale, la nuit du passage à l'heure d'hiver contient deux fois 2h-3h).
- **Session Spark en UTC** : `spark.sql.session.timeZone = UTC`.
- **Dédoublonnage déterministe** : `row_number` sur (région, `date_heure`), en gardant d'abord la ligne dont le libellé `heure` correspond à l'heure locale recalculée depuis `date_heure`, puis le définitif avant le consolidé.
- **Comptage avant / après dédoublonnage** journalisé. Valeur attendue : 24 lignes supprimées par mois de mars présent dans la plage (144 pour 2021-2026). Tout autre chiffre signale un changement de comportement de la source.
- **Nullification du détail éolien avant 2024** (voir qualité des données).
- **`MERGE` idempotent** avec `row_hash` (SHA-256) : seules les lignes réellement modifiées sont mises à jour (typiquement le passage de consolidé à définitif). Le hash remplace les nulls par une sentinelle avant concaténation, car `concat_ws` ignore les nulls et rendrait `[A, null, B]` et `[A, B, null]` indiscernables.
- **Fidélité à la source** : une ligne publiée vide reste vide en silver ; si RTE la complète, le `row_hash` change et le `MERGE` la met à jour automatiquement.
- **Pas de partitionnement** : environ 1,2 million de lignes ; partitionner créerait des petits fichiers sans gain.
- **Chemins OneLake dynamiques** : le chemin du bronze est construit depuis `notebookutils.runtime.context["currentWorkspaceName"]`, pour que le notebook fonctionne sans modification dans `elec-test`.

### `nb_silver_meteo` → `meteo_horaire`

- Lecture du JSON multiligne, aplatissement des tableaux parallèles par `arrays_zip` + `explode`.
- Code région récupéré depuis le dossier `region=XX` (découverte de partitions).
- Même stratégie de `MERGE` sur (région, `ts_utc`).

---

## Couche Gold

### Tables

| Table | Grain | Colonnes |
|---|---|---|
| `fact_production_horaire` | région × heure UTC × filière | `code_region`, `heure_utc`, `filiere`, `mwh`, `nb_pas`, `annee` |
| `fact_conso_meteo_horaire` | région × heure UTC | `code_region`, `heure_utc`, `conso_mwh`, `nb_pas`, `temperature_c`, `vent_kmh`, `rayonnement_wm2`, `annee` |
| `agg_mix_journalier` | région × jour local × filière | `code_region`, `date_locale`, `filiere`, `mwh`, `mwh_total_jour`, `part`, `nb_heures`, `annee` |

### Choix communs

- **Grain horaire** : nécessaire pour joindre la météo (horaire) et suffisant pour l'analyse.
- **MW → MWh** : éCO2mix publie une puissance (MW). Sur un pas demi-heure, énergie = MW × 0,5 h ; agrégation par `date_trunc('hour', ts_utc)`.
- **`nb_pas`** : nombre de demi-heures non nulles dans l'heure (2 attendu). Une heure incomplète reste exacte en énergie mesurée mais devient visible et requêtable.
- **Pas d'imputation** : une demi-heure absente ou vide à la source n'est pas estimée ; l'heure garde l'énergie mesurée avec `nb_pas = 1`. Le contrôle de conservation reste ainsi exact.
- **Écriture par `replaceWhere`** sur `annee >= annee_debut` : le gold est intégralement recalculable depuis le silver, la tranche concernée est remplacée de façon atomique. `MERGE` en silver, `replaceWhere` en gold. Le DataFrame écrit est filtré sur `annee_debut` en entrée, sinon Delta refuse l'écriture.
- **Coût du `replaceWhere` sans partitionnement** : observé avec `annee_debut = 2025`, Delta a retiré les 9 fichiers et recopié les 2,9 millions de lignes 2021-2024 (`numCopiedRows`) pour remplacer 1,1 million de lignes. Négligeable sur 22 Mo ; le partitionnement par `annee` ne deviendrait rentable que sur des volumes bien plus gros.

### `fact_production_horaire`

- Format long obtenu par `unpivot`, puis filtre `mw IS NOT NULL`.
- La constante `FILIERES` définit une **partition** de la production (chaque MWh dans une seule filière) : thermique, nucléaire, éolien (total), solaire, hydraulique, bioénergies, déstockage batterie.
- Exclus : consommation, pompage, stockage batterie, échanges physiques (pas de la production), taux `tco_*` / `tch_*` (des pourcentages, pas des MW), détail éolien terrestre / offshore (double comptage avec le total).

### `fact_conso_meteo_horaire`

- Consommation agrégée à l'heure, puis jointures **left** vers la météo : une heure sans météo reste visible avec des nulls au lieu de disparaître.
- **Alignement temporel** : température et vent (valeurs instantanées) sont joints sur l'heure `h` ; le rayonnement (moyenne sur l'heure précédente chez Open-Meteo) est joint sur `h + 1 h`.
- Le décalage se fait par **jointure sur l'horodatage décalé**, pas par `lead` : `lead` prend la ligne suivante et irait chercher la mauvaise heure en cas de trou, sans rien signaler.

### `agg_mix_journalier`

- Lue depuis `fact_production_horaire` (déjà contrôlée), pas depuis le silver.
- **Jour local** : `date_locale = to_date(from_utc_timestamp(heure_utc, 'Europe/Paris'))`. Le filtre d'entrée et `annee` portent sur `date_locale`, pour qu'un `replaceWhere` ne coupe jamais une journée en deux.
- `mwh_total_jour` calculé par fenêtre (`SUM() OVER (PARTITION BY code_region, date_locale)`), `part = mwh / mwh_total_jour`.
- **Une part n'est pas additive** : `mwh` et `mwh_total_jour` sont conservés pour recalculer une part sur n'importe quelle période (rapport des sommes, jamais moyenne de parts). Dans Power BI, la part sera une mesure DAX.

---

## Contrôles et journalisation

### Principe

- Les contrôles s'exécutent **avant l'écriture** : une donnée fausse n'atteint jamais le gold.
- Ils vérifient des **invariants** (propriétés toujours vraies), jamais des valeurs codées en dur qui deviendraient fausses avec l'arrivée de nouvelles données.
- Les échecs sont **accumulés** puis levés ensemble (`raise ValueError`, pas `assert`), avec le nombre de cas et quelques exemples.
- Deux niveaux : **bloquant** (le pipeline échoue) et **avertissement** (journalisé, le pipeline continue).
- Les contrôles exploratoires (corrélations, ordres de grandeur) servent à valider une fois et ne sont pas des barrières.

### Contrôles par table

| Table | Contrôle | Niveau |
|---|---|---|
| `fact_production_horaire` | `FILIERES` ne contient aucune colonne interdite (consommation, stockage, échanges, détail éolien, `tco_*`, `tch_*`) | Bloquant |
| | Clé unique (région, heure, filière) | Bloquant |
| | Conservation : Σ MWh gold = Σ filières silver × 0,5 sur la même tranche | Bloquant |
| | Heures avec `nb_pas ≠ 2` : au-delà de 24 couples (région, heure) | Bloquant |
| | Heures avec `nb_pas ≠ 2` : jusqu'à 24 couples | Avertissement |
| `fact_conso_meteo_horaire` | Table non vide | Bloquant |
| | Pas de démultiplication : nombre de lignes après jointures météo = avant | Bloquant |
| | Clé unique (région, heure) | Bloquant |
| | 0 null dans les trois colonnes météo | Bloquant |
| | Nombre de régions = régions `perimetre_eco2mix` du référentiel | Bloquant |
| | Heures incomplètes : même règle à deux niveaux que la production | Bloquant / Avertissement |
| `agg_mix_journalier` | Clé unique (région, jour local, filière) | Bloquant |
| | Les parts somment à 1 par (région, jour), tolérance 1e-9, parts nulles détectées | Bloquant |
| | Conservation par jour local avec `fact_production_horaire` (jointure `full_outer`, tolérance 0,001 MWh) | Bloquant |
| | `nb_heures` = heures civiles du jour (23 / 24 / 25), plafonnées à 24 (heure d'octobre absente à la source), premier jour de la plage exclu | Bloquant |

Le contrôle des filières interdites vérifie **la liste elle-même** : un contrôle « filière hors liste » sur la sortie de l'`unpivot` ne peut jamais échouer, et la conservation non plus, puisqu'elle utilise la même liste. C'est le seul qui aurait détecté le double comptage de l'éolien.

### Fonctions partagées (`nb_utils`, inclus via `%run nb_utils`)

| Fonction | Rôle |
|---|---|
| `row_hash(cols)` | SHA-256 des colonnes avec sentinelle pour les nulls |
| `write_slice(df, table, annee_debut)` | Création de la table au premier passage, puis `replaceWhere` |
| `log_execution(table, statut, lignes, message)` | Ajout d'une ligne au journal |
| `lever_si_echecs(erreurs, table)` | Lève une erreur regroupant tous les contrôles en échec |
| `publier(df, table, controles, annee_debut)` | Cache → contrôles → écriture → log succès ; log échec puis `raise` en cas d'erreur ; libération du cache dans `finally` |

`publier` reçoit la fonction de contrôle **en paramètre** (l'équivalent d'un délégué `Func<DataFrame, List<string>>` en C#) et l'appelle dans son `try` : un contrôle qui plante est journalisé comme un contrôle en échec. Les fonctions de contrôle, spécifiques à chaque table, vivent dans `nb_gold`.

### Journal des exécutions : `lh_admin.dbo.log_execution`

| Colonne | Contenu |
|---|---|
| `run_id` | `RunId` de `pl_main` (ou `manuel` en interactif) |
| `notebook` | Nom du notebook |
| `table_cible` | Table traitée |
| `statut` | `succes`, `echec`, `avertissement` |
| `lignes` | `numOutputRows` lu dans l'historique Delta (pas de `count()` supplémentaire) |
| `message` | Erreurs ou avertissement (tronqué à 1 000 caractères) |
| `horodatage_utc` | Instant de la journalisation |

Le `raise` après la journalisation d'un échec est indispensable : sans lui, l'erreur serait avalée et le pipeline afficherait une réussite.

`notebookutils.notebook.exit` n'est appelé **qu'une fois, dans la dernière cellule**, pour renvoyer un résumé JSON au pipeline : il termine immédiatement le notebook, et toutes les cellules suivantes seraient marquées annulées.

---

## Qualité des données : constats et règles

| Constat | Analyse | Règle appliquée |
|---|---|---|
| `tch_nucleaire` null dans 5 régions | Régions sans centrale nucléaire : capacité installée nulle, taux de charge indéfini ; la production y vaut bien 0 | Null conservé : il a un sens métier |
| Détail éolien terrestre + offshore = 0 de 2021 à 2023 | Période continue (densité ~75 % sur 2021-2024) : ventilation non publiée avant 2024, remplie par des **faux zéros** | Détail nullifié avant le 01/01/2024 en silver |
| Écarts de 1 MW entre `eolien` et le détail depuis 2024 | Surtout quand l'offshore produit : signature d'un arrondi séparé des composantes | Tolérance documentée, `eolien` fait foi |
| Écarts de plus de 1 MW (max 20 MW), détail toujours supérieur au total | Uniquement quand l'offshore vaut 0 et la production est faible ; hypothèse cohérente : consommation propre des parcs en mer à l'arrêt, déduite du total mais pas reportée en négatif | `eolien` fait foi ; le détail reste en silver pour une analyse spécifique |
| Écarts limités aux régions avec éolien en mer | Bretagne, Normandie, Pays de la Loire (parcs posés) ; PACA et Occitanie (fermes pilotes flottantes, écarts de 1 MW seulement) | — |
| Passage à l'heure d'été (mars) : 2 doublons par région | La source publie toujours 48 créneaux locaux ; les libellés 02:00 / 02:30 inexistants tombent sur le même instant UTC que 03:00 / 03:30. Valeurs identiques à la vraie ligne | Dédoublonnage déterministe sur la cohérence de l'heure locale |
| Passage à l'heure d'hiver (octobre) : 1 heure manquante par région et par an | Grille fixe de 48 créneaux pour une journée de 25 h : la première occurrence de 02h-03h (00:00-01:00 UTC) n'est pas publiée | Perte à la source documentée, non imputée (impact négligeable sur les totaux) |
| Ligne vide le 31/12/2025 à 23:00 UTC (minuit à Paris le 01/01/2026), 12 régions | Ligne présente mais toutes mesures nulles, à la jonction entre données définitives 2025 et consolidées 2026 (RTE découpe ses publications par année locale) : artefact de publication | Conservée en silver ; heure gold à `nb_pas = 1` ; avertissement journalisé ; corrigée automatiquement par le `MERGE` si RTE la complète |

Leçon générale : **une ligne présente n'est pas une donnée présente** ; un null peut avoir un sens métier, et un zéro peut mentir.

---

## Analyses de validation

### Alignement du rayonnement

Profil de corrélation production solaire / rayonnement, calculé par région et par année puis moyenné :

| Décalage | -1 h | 0 | **+1 h** | +2 h |
|---|---|---|---|---|
| Corrélation moyenne | 0,850 | 0,937 | **0,942** | 0,864 |

Pic à +1 h, conforme à la documentation d'Open-Meteo. Une interpolation parabolique place le sommet vers **+0,56 h** : incertitude résiduelle d'environ 30 min, probablement liée à la convention d'horodatage d'éCO2mix. Décision : décalage de +1 h.

La même corrélation calculée globalement (toutes régions et années mélangées) ne donne que 0,67 : agréger des groupes hétérogènes (puissances installées différentes, parc solaire en croissance) masque une relation très forte à l'intérieur de chaque groupe.

### Consommation / température

Corrélation négative dans toutes les régions (chauffage électrique), de -0,71 (Normandie) à -0,41 (PACA) et -0,46 (Occitanie). Les régions méditerranéennes, plus faibles, ont vraisemblablement une relation non linéaire (consommation qui remonte l'été avec la climatisation), que la corrélation de Pearson capture mal.

### Production annuelle (12 régions)

| Année | Gold |
|---|---|
| 2021 | ~520 TWh |
| 2022 | ~443 TWh (indisponibilité d'une partie du parc nucléaire) |
| 2023 | ~493 TWh |

Ces valeurs sont légèrement inférieures aux bilans nationaux de RTE, l'écart correspondant à la Corse. Ce contrôle d'ordre de grandeur a révélé le double comptage de l'éolien en 2024 (≈ +47 TWh) avant sa correction.

---

## Tests de validation

| Test | Attendu |
|---|---|
| Relance complète de l'orchestration bronze | Mêmes fichiers, aucun doublon |
| `DESCRIBE HISTORY eco2mix_regional` après une deuxième exécution | 0 ligne insérée, 0 mise à jour |
| Lignes supprimées par le dédoublonnage silver | 24 par mois de mars dans la plage |
| Jours UTC avec un nombre de demi-heures ≠ 48 | Uniquement le dernier dimanche d'octobre (46) et le dernier jour de la plage |
| `replaceWhere` avec `annee_debut = 2021` puis `2025` | Les deux exécutions passent, nombre de lignes inchangé |
| Contrôles gold | Ajouter temporairement `eolien_terrestre` à `FILIERES` → échec de `nb_gold` et ligne `echec` dans `log_execution` |
| `pl_main` de bout en bout | Une ligne `succes` par table gold, toutes avec le même `run_id` |

---

## Capacité trial : enseignements

- Une capacité trial n'a **ni file d'attente ni burst** pour Spark : un job qui ne trouve pas assez de cœurs est rejeté (`TooManyRequestsForCapacity`, HTTP 430) au lieu d'attendre, contrairement à une capacité F payante.
- Parades appliquées : notebooks silver en **séquence** plutôt qu'en parallèle, **retry** sur les activités Notebook, **délai d'expiration des sessions** réduit à 10 min.
- Options étudiées : pool personnalisé à petits nœuds (fonctionne, mais doublait la durée du pipeline), mode haute concurrence avec *session tag* partagé (bonus).
- L'admission optimiste (paramètre « Réserver le nombre maximal de cœurs » désactivé) admet un job sur son minimum de nœuds puis le laisse grossir.

---

## Maintenance des tables (prévu)

- Pas d'`OPTIMIZE` dans les notebooks de chargement : chaque `replaceWhere` réécrit déjà le gold, un compactage immédiat doublerait le coût.
- Prévu : un `nb_maintenance` planifié (`OPTIMIZE` puis `VACUUM`, rétention par défaut de 7 jours) et l'*optimize write* sur les tables gold.
- Le silver est la couche la plus exposée à la fragmentation (petits fichiers à chaque `MERGE`).
- À vérifier pour l'étape 5 : V-Order sur les tables gold lues en Direct Lake.

---

## Feuille de route

| Étape | Contenu | État |
|---|---|---|
| 1 | Socle, Git, ingestion bronze | ✅ |
| 2 | Médaillon Spark (silver, gold), contrôles, journalisation, orchestration `pl_main` | ✅ |
| 3 | Real-Time Intelligence : producteur Python, Eventstream, Eventhouse / KQL, Real-Time Dashboard, Activator | ⏳ |
| 4 | Warehouse : modèle en étoile T-SQL, procédures, sécurité (CLS, DDM) | ⏳ |
| 5 | Power BI : modèle sémantique Direct Lake, RLS par région | ⏳ |
| 6 | Deployment pipeline dev → test, variable libraries, OneLake security, monitoring | ⏳ |

### Bonus de l'étape 2 non réalisés

- `nb_maintenance` et propriétés *optimize write*.
- Généraliser `publier` au silver en lui passant aussi la fonction d'écriture (`MERGE`).
- Expérience petits fichiers / `OPTIMIZE` / V-Order chronométrée.
- `notebookutils.notebook.runMultiple` comme alternative à l'orchestration par pipeline.
- Mode haute concurrence pour les pipelines.

---

## Points d'attention

- Les **connexions** Fabric ne sont pas versionnées dans Git (référencées par ID) : à recréer ou paramétrer par environnement (étape 6).
- Git ne versionne que les **définitions** des items, pas les données. Les sources étant en open data, tout est reconstructible en relançant les pipelines.
- Projet réalisé sur une capacité **trial** Fabric.
