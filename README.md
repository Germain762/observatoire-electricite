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
```

Architecture médaillon :

- **Bronze** : fichiers bruts tels que livrés par les API (Parquet éCO2mix, JSON météo), plus le référentiel régions.
- **Silver** : données typées, dédoublonnées, clé en UTC, chargées par `MERGE` idempotent.
- **Gold** : tables analytiques au grain horaire et journalier.

---

## Environnements et organisation

| Élément | Choix |
|---|---|
| Workspaces | `elec-dev` (développement), `elec-test` (cible du deployment pipeline) |
| Git | Azure DevOps ; `elec-dev` synchronisé sur la branche `dev` ; `main` n'avance que par pull request depuis `dev` |
| Dossier Git | `/fabric` pour les items Fabric, la racine reste libre pour la doc et le code hors Fabric |
| Format des modèles sémantiques | Grands modèles sémantiques, identique sur les deux workspaces |
| Dossiers du workspace | Un dossier par couche (`bronze`, `silver`, `gold`) |

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

Colonnes temporelles : le fuseau est toujours explicite dans le nom (`ts_utc`, `ts_paris`, `heure_utc`).

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
- **Incrémental** : relancer avec `annee_debut` = année précédente, car les données consolidées deviennent définitives au deuxième trimestre A+1.
- **Copie en Binary** : le Parquet de l'API est recopié tel quel, sans parsing dans le pipeline.
- **Pas de ForEach imbriqué** (non supporté dans les pipelines Fabric) : découpage en pipelines enfants appelés par Invoke Pipeline.
- **Corse hors périmètre** : la Corse n'est pas raccordée au réseau continental et n'apparaît pas dans éCO2mix régional. Elle reste dans le référentiel avec `perimetre_eco2mix = false` ; chaque traitement filtre selon ses besoins, rien n'est exclu en dur.
- **Contrôle qui échoue bruyamment** : le notebook de contrôle lève une exception, le pipeline voit l'échec et déclenche l'alerte. Test : passer la Corse à `true` doit faire échouer le contrôle.

---

## Couche Silver

### `nb_silver_eco2mix`

- **Clé** : (`code_insee_region`, `ts_utc`). L'UTC est la seule référence temporelle fiable ; `ts_paris` n'est qu'un attribut (à l'heure locale, la nuit du passage à l'heure d'hiver contient deux fois 2h-3h).
- **Session Spark en UTC** : `spark.sql.session.timeZone = UTC`.
- **Dédoublonnage déterministe** : `row_number` sur (région, `date_heure`), en gardant d'abord la ligne dont le libellé `heure` correspond à l'heure locale recalculée depuis `date_heure`, puis le définitif avant le consolidé.
- **Comptage avant / après dédoublonnage** journalisé. Valeur attendue : 24 lignes supprimées par mois de mars présent dans la plage (144 pour 2021-2026). Tout autre chiffre signale un changement de comportement de la source.
- **Nullification du détail éolien avant 2024** (voir qualité des données).
- **`MERGE` idempotent** avec `row_hash` (SHA-256) : seules les lignes réellement modifiées sont mises à jour. Le hash remplace les nulls par une sentinelle avant concaténation, car `concat_ws` ignore les nulls et rendrait `[A, null, B]` et `[A, B, null]` indiscernables.
- **Pas de partitionnement** : environ 1,2 million de lignes ; partitionner créerait des petits fichiers sans gain.
- **Chemins OneLake dynamiques** : le chemin du bronze est construit depuis `notebookutils.runtime.context["currentWorkspaceName"]`, pour que le notebook fonctionne sans modification dans `elec-test`.

### `nb_silver_meteo`

- Lecture du JSON multiligne, aplatissement des tableaux parallèles par `arrays_zip` + `explode`.
- Code région récupéré depuis le dossier `region=XX` (découverte de partitions).
- Même stratégie de `MERGE` sur (région, `ts_utc`).

### Fonctions partagées

`row_hash` et les utilitaires communs sont centralisés dans `nb_utils`, inclus via `%run nb_utils`.

---

## Couche Gold (en cours)

- **Grain horaire** : nécessaire pour joindre la météo (horaire) et suffisant pour l'analyse.
- **MW → MWh** : éCO2mix publie une puissance (MW). Sur un pas demi-heure, énergie = MW × 0,5 h ; agrégation par `date_trunc('hour', ts_utc)`.
- **`fact_production_horaire`** au format long (région, heure, filière, MWh), obtenu par `unpivot`. Les filières forment une **partition** de la production (chaque MWh dans une seule filière) : thermique, nucléaire, éolien (total), solaire, hydraulique, bioénergies, déstockage batterie. Sont exclus : consommation, pompage, stockage batterie, échanges physiques (pas de la production), taux `tco_*` / `tch_*` (des pourcentages, pas des MW), détail éolien terrestre / offshore (double comptage avec le total).
- **`fact_conso_meteo_horaire`** : consommation horaire jointe à la météo sur (région, `heure_utc`).
- **`agg_mix_journalier`** : parts de filière calculées comme rapport des sommes (jamais moyenne de parts horaires), par **jour local** (date de `ts_paris`).
- **Écriture par `replaceWhere`** sur `annee >= annee_debut` : le gold est intégralement recalculable depuis le silver, la tranche concernée est remplacée de façon atomique. `MERGE` en silver, `replaceWhere` en gold.

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

---

## Tests de validation

| Test | Attendu |
|---|---|
| Relance complète de l'orchestration bronze | Mêmes fichiers, aucun doublon |
| `DESCRIBE HISTORY eco2mix_regional` après une deuxième exécution | 0 ligne insérée, 0 mise à jour |
| Lignes supprimées par le dédoublonnage silver | 24 par mois de mars dans la plage |
| Jours UTC avec un nombre de demi-heures ≠ 48 | Uniquement le dernier dimanche d'octobre (46) et le dernier jour de la plage |
| Consommation annuelle totale des 12 régions | De l'ordre de 450 TWh |
| Heures présentes par jour local dans `agg_mix_journalier` | 23 le dernier dimanche de mars, 24 le dernier dimanche d'octobre (heure manquante à la source), 24 sinon |

---

## Feuille de route

| Étape | Contenu | État |
|---|---|---|
| 1 | Socle, Git, ingestion bronze | ✅ |
| 2 | Médaillon Spark (silver, gold) | 🔄 silver eco2mix validé, gold en cours |
| 3 | Real-Time Intelligence : producteur Python, Eventstream, Eventhouse / KQL, Real-Time Dashboard, Activator | ⏳ |
| 4 | Warehouse : modèle en étoile T-SQL, procédures, sécurité (CLS, DDM) | ⏳ |
| 5 | Power BI : modèle sémantique Direct Lake, RLS par région | ⏳ |
| 6 | Deployment pipeline dev → test, variable libraries, OneLake security, monitoring | ⏳ |

---

## Points d'attention

- Les **connexions** Fabric ne sont pas versionnées dans Git (référencées par ID) : à recréer ou paramétrer par environnement (étape 6).
- Git ne versionne que les **définitions** des items, pas les données. Les sources étant en open data, tout est reconstructible en relançant les pipelines.
- Projet réalisé sur une capacité **trial** Fabric.
