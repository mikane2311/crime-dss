# 🔍 Crime DSS — Système d'Information Décisionnel
### Analyse de la Criminalité à Los Angeles (2020–2024)

> **Problématique :** Comment la criminalité à Los Angeles évolue-t-elle entre 2020 et 2024 selon le temps et la zone géographique, et comment cette évolution peut-elle être interprétée à la lumière de la population et du taux de chômage ?

📹 **Vidéo de démonstration (10 min) :** [YouTube ](https://youtu.be/8r5c-E52YT4)

---

## 📋 Table des matières

- [Aperçu du projet](#aperçu-du-projet)
- [Architecture technique](#architecture-technique)
- [Structure du projet](#structure-du-projet)
- [Sources de données](#sources-de-données)
- [Pipeline ETL](#pipeline-etl)
- [Base de données](#base-de-données)
- [Dashboard Power BI](#dashboard-power-bi)
- [Automatisation Airflow](#automatisation-airflow)
- [Optimisation SQL](#optimisation-sql)
- [Résultats clés](#résultats-clés)
- [Recommandations stratégiques](#recommandations-stratégiques)
- [Installation et démarrage](#installation-et-démarrage)

---

## 📌 Aperçu du projet

Ce projet est un **Système d'Information Décisionnel (SID)** complet construit en 4 semaines, couvrant l'ensemble du cycle data engineering :

| Semaine | Objectif | Livrables |
|---------|----------|-----------|
| 1 | Cadrage et modélisation conceptuelle | Schéma conceptuel, dictionnaire de données |
| 2 | Implémentation base relationnelle | PostgreSQL, ETL Python, 1M+ lignes chargées |
| 3 | Exploitation analytique | Vues SQL, KPI, Dashboard Power BI |
| 4 | Automatisation et optimisation | Airflow DAG, index SQL, procédures stockées |

---

## 🏗️ Architecture technique

```
┌─────────────────────────────────────────────────────────┐
│                     Sources de données                  │
│  LAPD Crime Data │ U.S. Census Bureau │ BLS LAUS        │
└────────────────────────┬────────────────────────────────┘
                         │ ETL Python (pandas + psycopg2)
                         ▼
┌─────────────────────────────────────────────────────────┐
│              PostgreSQL (Docker)                        │
│  fact_crime (1M+) │ dim_location │ dim_time             │
│  population       │ unemployment │ vues analytiques     │
└────────────────────────┬────────────────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          ▼                             ▼
┌──────────────────┐         ┌──────────────────────┐
│   Power BI       │         │  Apache Airflow      │
│   Dashboard      │         │  (Docker)            │
│   interactif     │         │  DAG quotidien 06:00 │
└──────────────────┘         └──────────────────────┘
```

**Outils :** Python · SQL · PostgreSQL · Docker · Apache Airflow · Power BI · WSL

---

## 📁 Structure du projet

```
crime-dss/
├── data/
│   ├── raw/                    # Données brutes (non versionnées)
│   └── processed/              # Données nettoyées (non versionnées)
├── etl/
│   └── etl_pipeline.py         # Script ETL principal
├── sql/
│   ├── week3_advanced.sql      # Requêtes analytiques avancées
│   └── week4_optimized.sql     # Script optimisé + index + procédures
├── airflow/
│   └── dags/
│       └── crime_etl_dag.py    # DAG Airflow
├── README.md
└── .gitignore
```

---

## 📊 Sources de données

| Source | Description | Format |
|--------|-------------|--------|
| [LAPD Crime Data](https://data.lacity.org/) | Crimes à Los Angeles 2020–2024 | CSV |
| [U.S. Census Bureau](https://www.census.gov/) | Population annuelle Californie | XLSX |
| [BLS LAUS](https://www.bls.gov/lau/) | Taux de chômage mensuel | CSV |

**Fenêtre temporelle :** 2020–2024 (2025 exclu — données partielles)

---

## 🔄 Pipeline ETL

Le script `etl/etl_pipeline.py` réalise :

1. **Connexion** à PostgreSQL
2. **Contrôle qualité** — NULLs, doublons, orphelins
3. **Rafraîchissement KPI** via procédure stockée
4. **Vérification** et logging des résultats

```bash
cd etl
python etl_pipeline.py
```

---

## 🗄️ Base de données

### Schéma relationnel

```

```

### Chargement

| Table | Lignes |
|-------|--------|
| dim_time | 5 |
| dim_location | 21 |
| population | 5 |
| unemployment | 5 |
| **fact_crime** | **1 004 894** |

### Démarrer PostgreSQL

```bash
docker run -d \
  --name crime_db \
  -e POSTGRES_USER=crime_user \
  -e POSTGRES_PASSWORD=crime_pass \
  -e POSTGRES_DB=crime_db \
  -p 5432:5432 \
  postgres:15
```

---

## 📈 Dashboard Power BI

Connexion directe à PostgreSQL via les 4 vues SQL :

| Vue | Description |
|-----|-------------|
| `vw_crime_yearly_kpi` | KPI annuels : crimes, population, chômage, crime rate |
| `vw_crime_by_location` | Crimes par zone géographique |
| `vw_crime_by_type` | Crimes par type d'infraction |
| `vw_crime_year_location` | Crimes par zone et par année |
| `vw_crime_by_location_geo` | Coordonnées moyennes par zone (carte) |

**Visuels :** 7 KPI cards · Combo chart · Line chart · Bar chart · Treemap · Matrix heatmap · Carte géographique

---

## ⚙️ Automatisation Airflow

### Démarrer Airflow

```bash
docker run -d \
  --name airflow \
  -p 8080:8080 \
  -e AIRFLOW__CORE__EXECUTOR=SequentialExecutor \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db \
  -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
  apache/airflow:2.9.0 standalone
```

### Copier le DAG

```bash
docker cp airflow/dags/crime_etl_dag.py airflow:/opt/airflow/dags/
```

### Pipeline DAG

```
start → check_connection → quality_checks → refresh_kpi → verify_kpi → end
```

**Schedule :** tous les jours à 06:00 — `0 6 * * *`

Interface : [http://localhost:8080](http://localhost:8080)

---

## ⚡ Optimisation SQL

### Index créés sur `fact_crime`

```sql
CREATE INDEX idx_fact_crime_year           ON fact_crime(year);
CREATE INDEX idx_fact_crime_location_id    ON fact_crime(location_id);
CREATE INDEX idx_fact_crime_crime_desc     ON fact_crime(crime_desc);
CREATE INDEX idx_fact_crime_year_location  ON fact_crime(year, location_id);
```

### Gains obtenus

| Métrique | Avant | Après |
|----------|-------|-------|
| Scan type | Seq Scan | Index Only Scan |
| Heap Fetches | N/A | **0** |
| Disk sort | ~10 MB/worker | ~2–4 MB/worker |

---

## 🔑 Résultats clés

| Année | Total crimes | Crime Rate/100k | Chômage (%) | Variation |
|-------|-------------|-----------------|-------------|-----------|
| 2020 | 199 847 | 5 129.11 | 10.17 | — |
| 2021 | 209 876 | 5 478.38 | 7.34 | +5.02% |
| 2022 | **235 259** | **6 137.73** | 4.27 | +12.09% |
| 2023 | 232 345 | 6 038.97 | 4.72 | -1.24% |
| 2024 | 127 567 | 3 288.91 | 5.32 | **-45.10%** |

**Top 3 zones :** Central (69 668) · 77th Street (61 756) · Pacific (59 513)

**Top 3 crimes :** Vehicle Stolen (115 184) · Battery Simple Assault (74 821) · Burglary From Vehicle (63 515)

---

## 💡 Recommandations stratégiques

1. **Renforcer la surveillance** dans les zones Central et 77th Street — points chauds récurrents sur toute la période
2. **Cibler la prévention** du vol de véhicule et des agressions simples — plus de 190 000 incidents cumulés
3. **Exploiter la baisse de 45.8%** en 2024 pour évaluer et renforcer les politiques de sécurité récentes du LAPD
4. **Industrialiser le pipeline** — remplacer SQLite par PostgreSQL comme backend Airflow, intégrer de nouvelles sources

---

## 🚀 Installation et démarrage

### Prérequis

- Docker Desktop
- WSL (Ubuntu)
- Python 3.10+
- Power BI Desktop (Windows)

### Démarrage rapide

```bash
# 1. Cloner le repo
git clone https://github.com/mikane2311/crime-dss.git
cd crime-dss

# 2. Installer les dépendances Python
pip install pandas psycopg2-binary sqlalchemy

# 3. Démarrer PostgreSQL
docker run -d --name crime_db \
  -e POSTGRES_USER=crime_user \
  -e POSTGRES_PASSWORD=crime_pass \
  -e POSTGRES_DB=crime_db \
  -p 5432:5432 postgres:15

# 4. Exécuter le pipeline ETL
cd etl && python etl_pipeline.py

# 5. Lancer les scripts SQL
psql -h localhost -U crime_user -d crime_db -f sql/week4_optimized.sql
```

---

## 👤 Auteur

**[MIKANE Fatima-Ezzhrae]** — INPT, Rabat, Maroc

Année universitaire : 2025–2026

---

