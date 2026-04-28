# AniData Lab — Observatoire intelligent du marché Anime / Manga

**Projet fil rouge — Sakura Analytics**
Formation Data Engineering | Semaine du 23 au 27 mars 2026


---

## Contexte du projet

Dans ce projet, j'incarne un **Data Engineer** au sein d'un studio d'animation japonais fictif appelé **Sakura Analytics**. L'objectif est de construire une plateforme de données complète de bout en bout, à partir de données brutes issues de **MyAnimeList** — le plus grand site de référencement d'animes et mangas au monde.

Les données utilisées :
- **17 562 animes** avec leurs genres, studios, scores, épisodes, statuts...
- **57 millions de ratings** donnés par 310 059 utilisateurs
- **Synopsis textuels** de chaque anime

Le livrable final est un **pipeline ETL automatisé** (Extract → Transform → Load) orchestré avec Apache Airflow, qui alimente un dashboard Grafana interactif.

---

## Stack technique

| Composant | Technologie |
|-----------|------------|
| Langage | Python 3.10+ |
| Manipulation données | Pandas, NumPy |
| Big Data / Recherche | Elasticsearch, Logstash, Grafana (ELK 8.x) |
| Orchestration ETL | Apache Airflow 2.x |
| Conteneurisation | Docker, Docker Compose |

---

## Structure du projet

```
anidata-lab/
├── data/
│   ├── raw/          ← Fichiers CSV bruts (MyAnimeList)
│   └── gold/         ← Dataset nettoyé et enrichi (produit par nos scripts)
├── notebooks/        ← Notebooks Jupyter (audit + nettoyage)
├── src/
│   ├── extract/      ← Module Python : chargement des CSV
│   ├── transform/    ← Module Python : nettoyage et feature engineering
│   └── load/         ← Module Python : indexation Elasticsearch
├── dags/             ← DAGs Apache Airflow
├── docker/
│   ├── elk/          ← Docker Compose pour Elasticsearch + Logstash + Grafana
│   └── airflow/      ← Docker Compose pour Airflow
└── Makefile          ← Commandes rapides pour lancer les services
```

---

## Séance 1 — Audit & Découverte du dataset brut

### Ce que j'ai fait

La première étape de tout projet data est de **comprendre les données avant de les toucher**. On ne peut pas nettoyer ou transformer des données qu'on ne comprend pas. C'est ce qu'on appelle un **audit de qualité**.

J'ai créé un notebook Jupyter (`notebooks/01_audit_qualite.ipynb`) qui explore les 3 fichiers CSV du dataset MyAnimeList.

### Chargement des fichiers

J'ai chargé les 3 fichiers avec Pandas :

```python
anime_df    = pd.read_csv('data/raw/anime.csv')             # 17 562 animes
ratings_df  = pd.read_csv('data/raw/rating_complete.csv')   # 57M ratings
synopsis_df = pd.read_csv('data/raw/anime_with_synopsis.csv')
```

### Problèmes de qualité identifiés

Après exploration, j'ai identifié plusieurs problèmes :

**1. Valeurs manquantes**
Certaines colonnes comme `Score`, `Episodes`, `Genres` ou `Studios` contiennent des valeurs manquantes. En data engineering, une valeur manquante peut fausser tous les calculs si on ne la traite pas.

```python
# Fonction d'audit des valeurs manquantes
def audit_missing(df, name):
    missing = df.isnull().sum()
    pct = (missing / len(df) * 100).round(2)
    ...
```

**2. Valeurs "Unknown" au lieu de NaN**
Dans le CSV, les valeurs inconnues ne sont pas vides — elles contiennent la chaîne de caractères `"Unknown"`. Par exemple `Score = "Unknown"` au lieu d'être vide. C'est un problème car Pandas ne les reconnaît pas comme des valeurs manquantes automatiquement.

**3. Types incohérents**
La colonne `Score` est stockée comme du texte (`string`) alors qu'elle devrait être un nombre décimal (`float`). Même chose pour `Episodes`. On ne peut pas faire de calculs mathématiques sur du texte.

**4. Colonnes multi-valeurs**
Les colonnes `Genres` et `Studios` contiennent des listes séparées par des virgules dans une seule cellule. Par exemple : `"Action, Adventure, Comedy"`. Ce n'est pas exploitable directement pour des analyses par genre.

**5. Titres en japonais/coréen**
Certains titres d'animes contiennent des caractères non-ASCII (japonais, coréen). Il faut s'assurer que l'encodage UTF-8 est bien géré partout dans le pipeline.

### Livrable de la séance

Un notebook annoté avec le rapport d'audit listant tous les problèmes à corriger lors de la séance suivante.

---

## Séance 2 — Nettoyage, Enrichissement & Feature Engineering

### Ce que j'ai fait

Une fois l'audit terminé, j'ai appliqué les corrections identifiées et j'ai créé de nouvelles variables métier. J'ai travaillé dans le notebook `notebooks/02_nettoyage_feature_engineering.ipynb` et j'ai aussi écrit le code de façon modulaire dans `src/transform/transform_anime.py` pour pouvoir le réutiliser dans Airflow plus tard.

### Étape 1 : Suppression des doublons

```python
before = len(anime_df)
anime_df = anime_df.drop_duplicates(subset=['MAL_ID'])
print(f'Doublons supprimés : {before - len(anime_df)}')
```

On vérifie les doublons sur `MAL_ID` qui est l'identifiant unique de chaque anime. S'il y a deux lignes avec le même ID, on ne garde qu'une seule.

### Étape 2 : Remplacement de "Unknown" par NaN

```python
anime_df.replace('Unknown', np.nan, inplace=True)
```

Cette ligne remplace toutes les occurrences du texte `"Unknown"` par une vraie valeur manquante `NaN` (Not a Number). Après ça, Pandas peut les détecter et les traiter correctement.

### Étape 3 : Conversion des types

```python
anime_df['Score'] = pd.to_numeric(anime_df['Score'], errors='coerce')
anime_df['Episodes'] = pd.to_numeric(anime_df['Episodes'], errors='coerce')
```

Le paramètre `errors='coerce'` signifie : "si tu ne peux pas convertir, remplace par NaN au lieu de planter". C'est une bonne pratique pour les données brutes imprévisibles.

J'ai aussi calculé la colonne `Scored By` (nombre total de votes) en sommant les colonnes `Score-1` à `Score-10` qui représentent la distribution des votes :

```python
score_cols = [f'Score-{i}' for i in range(1, 11)]
anime_df['Scored By'] = anime_df[score_cols].sum(axis=1)
```

### Étape 4 : Parsing des colonnes multi-valeurs

```python
def parse_list_col(val):
    if pd.isna(val):
        return []
    return [x.strip() for x in str(val).split(',') if x.strip()]

anime_df['genres_list']   = anime_df['Genres'].apply(parse_list_col)
anime_df['studios_list']  = anime_df['Studios'].apply(parse_list_col)
```

Au lieu d'avoir `"Action, Adventure, Comedy"` dans une seule case, on obtient `["Action", "Adventure", "Comedy"]`. C'est une vraie liste Python qu'on peut analyser : compter les genres, filtrer par genre, etc.

### Étape 5 : Feature Engineering

Le **feature engineering** c'est la création de nouvelles variables à partir des données existantes pour apporter de la valeur métier.

**Score pondéré (Bayesian Average)**
Le problème d'un score simple : un anime avec 2 votes à 10/10 semble meilleur qu'un anime avec 100 000 votes à 9/10. Ce n'est pas représentatif. La formule bayésienne corrige ça en tenant compte du nombre de votes :

```
weighted_score = (votes / (votes + m)) × score + (m / (votes + m)) × moyenne_globale
```

Où `m = 5000` est le seuil minimum de votes considéré fiable. Plus un anime a de votes, plus son score pondéré se rapproche de son vrai score.

```python
C = anime_df['Score'].mean()  # moyenne globale de tous les animes
m = 5000
anime_df['weighted_score'] = (
    (anime_df['Scored By'] / (anime_df['Scored By'] + m)) * anime_df['Score'] +
    (m / (anime_df['Scored By'] + m)) * C
).round(4)
```

**Taux de complétion et d'abandon**
```python
anime_df['completion_rate'] = anime_df['Completed'] / total_interactions
anime_df['drop_rate']       = anime_df['Dropped']   / total_interactions
```

Ces deux métriques permettent de savoir si un anime est regardé jusqu'au bout ou abandonné en cours de route — un indicateur fort de la qualité perçue.

**Classification des studios**
```python
TOP_STUDIOS = ['Toei Animation', 'Sunrise', 'Madhouse', 'Bones', ...]

def classify_studio(studios):
    for s in studios:
        if s in TOP_STUDIOS:
            return 'Grand Studio'
    return 'Indépendant'
```

### Étape 6 : Fusion avec les synopsis

```python
gold_df = anime_df.merge(synopsis_df[['MAL_ID', 'sypnopsis']], on='MAL_ID', how='left')
```

On fait une jointure (comme un `JOIN` en SQL) pour ajouter le synopsis de chaque anime au dataset principal, en utilisant le `MAL_ID` comme clé commune.

### Étape 7 : Export du dataset gold

```python
gold_df.to_csv('data/gold/anime_gold.csv', index=False, encoding='utf-8')
gold_df.to_json('data/gold/anime_gold.json', orient='records', force_ascii=False)
```

Le dataset "gold" est la version propre et enrichie des données, prête à être utilisée par les outils suivants. On l'exporte en deux formats :
- **CSV** pour Logstash
- **JSON** pour Elasticsearch et Airflow

**Résultat : 17 562 animes avec 30+ colonnes enrichies.**

---

## Séance 3 — Découverte de la stack ELK & Indexation

### Ce que j'ai fait

Maintenant que le dataset gold est prêt, il faut le charger dans un moteur de recherche pour pouvoir l'interroger et le visualiser. J'ai déployé la **stack ELK** via Docker.

### C'est quoi ELK ?

**ELK** est un acronyme pour 3 outils qui fonctionnent ensemble :

- **E → Elasticsearch** : une base de données NoSQL orientée documents JSON, optimisée pour la recherche full-text et les agrégations sur de gros volumes. C'est notre "entrepôt de données" principal.

- **L → Logstash** : un pipeline de données. Il lit les fichiers CSV, transforme les données selon des règles définies, et les envoie dans Elasticsearch. C'est le pont entre nos fichiers et la base.

- **K → (Kibana / Grafana)** : l'outil de visualisation. Dans notre projet, on utilise **Grafana** à la place de Kibana car il offre des dashboards plus flexibles. Il se connecte à Elasticsearch et affiche nos données sous forme de graphiques interactifs.

### Pourquoi Elasticsearch plutôt que SQL ?

Avec 17 500 animes et 57 millions de ratings, une base SQL classique comme MySQL serait lente pour :
- Chercher dans les synopsis (recherche full-text)
- Calculer en temps réel le top des studios, la répartition des genres
- Alimenter un dashboard qui se met à jour automatiquement

Elasticsearch utilise un **index inversé** : au lieu de parcourir toutes les lignes, il sait immédiatement dans quels documents se trouve un mot. C'est ce qui le rend si rapide.

### Déploiement via Docker Compose

Au lieu d'installer Elasticsearch, Logstash et Grafana manuellement (ce qui prendrait des heures et dépend de l'OS), on utilise **Docker**. Docker permet de faire tourner des applications dans des **containers isolés** — des environnements identiques sur toutes les machines.

J'ai écrit un fichier `docker/elk/docker-compose.yml` qui définit les 3 services :

```yaml
services:
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.12.0
    environment:
      - discovery.type=single-node      # mode standalone (pas de cluster)
      - xpack.security.enabled=false    # sécurité désactivée pour le dev
      - ES_JAVA_OPTS=-Xms512m -Xmx512m  # limite la RAM utilisée
    ports:
      - "9200:9200"   # accessible sur http://localhost:9200

  logstash:
    image: docker.elastic.co/logstash/logstash:8.12.0
    volumes:
      - ./logstash/pipeline:/usr/share/logstash/pipeline  # nos règles de transformation
      - ../../data/gold:/data/gold                        # accès au dataset gold

  grafana:
    image: grafana/grafana:10.2.0
    ports:
      - "3000:3000"   # accessible sur http://localhost:3000
```

La commande `make elk` lance tout en une seule fois.

### Le mapping Elasticsearch

Comme une base SQL a un **schéma** (types des colonnes), Elasticsearch a un **mapping** qui définit comment chaque champ est stocké et indexé.

```python
ANIME_MAPPING = {
    "mappings": {
        "properties": {
            "Name":            {"type": "text"},      # recherche full-text
            "Score":           {"type": "float"},     # calculs numériques
            "genres_list":     {"type": "keyword"},   # filtres exacts
            "synopsis":        {"type": "text"},      # recherche full-text
            "studio_category": {"type": "keyword"},   # filtres exacts
        }
    }
}
```

La différence entre `text` et `keyword` :
- **`text`** : le contenu est découpé en mots et indexé pour la recherche full-text. Idéal pour les synopsis, les titres.
- **`keyword`** : la valeur est indexée telle quelle, pour des filtres exacts. Idéal pour les genres, les types (`TV`, `Movie`...).

### Le pipeline Logstash

J'ai configuré le fichier `docker/elk/logstash/pipeline/anime.conf` avec 3 sections :

**Input** — lire le CSV :
```
input {
  file {
    path => "/data/gold/anime_gold.csv"
    start_position => "beginning"
  }
}
```

**Filter** — transformer les données :
```
filter {
  csv { columns => ["MAL_ID", "Name", "Score", ...] }
  mutate {
    convert => { "Score" => "float", "Episodes" => "integer" }
  }
}
```

**Output** — envoyer dans Elasticsearch :
```
output {
  elasticsearch {
    hosts => ["http://elasticsearch:9200"]
    index => "anime-%{+YYYY.MM}"
    document_id => "%{MAL_ID}"
  }
}
```

Chaque ligne du CSV devient un **document JSON** dans l'index `anime-gold` d'Elasticsearch, identifié par son `MAL_ID`.

### Vérification

Après lancement, on peut vérifier qu'Elasticsearch fonctionne :

```bash
curl http://localhost:9200
# → { "name": "anidata-es", "version": { "number": "8.12.0" }, ... }
```

Et Grafana est accessible sur `http://localhost:3000` (admin / anidata123).

### Commandes utiles

```bash
make elk          # Lancer Elasticsearch + Logstash + Grafana
make stop-elk     # Arrêter la stack ELK
make status       # Voir l'état de tous les containers
```

---

---

## Séance 4 — Introduction à Apache Airflow & Premier DAG

### C'est quoi Apache Airflow ?

**Apache Airflow** est un outil d'**orchestration de pipelines de données**. Concrètement, il permet de définir des enchaînements de tâches automatisées, de les planifier, de les monitorer et de gérer les erreurs — le tout via une interface web.

L'analogie simple : Airflow c'est comme un **chef de cuisine** qui décide dans quel ordre les cuisiniers (les tâches) travaillent, surveille que personne ne bloque, et relance automatiquement si quelque chose échoue.

### Les concepts fondamentaux

**DAG (Directed Acyclic Graph)**
C'est le concept central d'Airflow. Un DAG est un **graphe de tâches avec des dépendances** :
- *Directed* : les tâches ont un sens (A → B → C)
- *Acyclic* : pas de boucle (on ne peut pas revenir en arrière)
- *Graph* : les tâches peuvent se ramifier et se rejoindre

Dans notre projet, chaque DAG représente une étape du pipeline ETL : extraction, transformation, chargement, détection d'anomalies.

**Task**
Une tâche unique dans un DAG. Par exemple : "lire le fichier anime.csv", "nettoyer les données", "envoyer dans Elasticsearch".

**Operator**
Le type d'une tâche. Dans notre projet on utilise principalement :
- `PythonOperator` → exécute une fonction Python
- `BranchPythonOperator` → choisit entre plusieurs chemins selon une condition
- `TriggerDagRunOperator` → déclenche un autre DAG

**Schedule**
La planification du DAG. On peut définir une expression cron (`0 2 * * *` = tous les jours à 2h) ou déclencher manuellement avec `schedule=None`.

**XCom (Cross-Communication)**
Le mécanisme qui permet aux tâches de s'échanger des données. Une tâche peut pousser une valeur avec `xcom_push` et une autre la récupérer avec `xcom_pull`.

```python
# Tâche A : pousser le nombre de lignes
context['ti'].xcom_push(key='anime_rows', value=len(df))

# Tâche B : récupérer cette valeur
rows = context['ti'].xcom_pull(task_ids='tache_a', key='anime_rows')
```

### Déploiement via Docker

Comme pour ELK, on utilise Docker Compose pour lancer Airflow. Notre configuration dans `docker/airflow/docker-compose.yml` lance 3 services :

- **postgres** : la base de données où Airflow stocke l'état de chaque DAG run (historique, logs, XComs)
- **airflow-webserver** : l'interface web accessible sur `http://localhost:8080`
- **airflow-scheduler** : le moteur qui surveille les DAGs et déclenche les tâches au bon moment

On utilise un **anchor YAML** (`x-airflow-common`) pour éviter de répéter la configuration commune (image, variables d'environnement, volumes) sur chaque service.

### Le DAG `hello_anidata`

Le premier DAG est un DAG de découverte qui introduit les concepts de base : log, branchement conditionnel, et paramètres.

```python
with DAG(
    dag_id='hello_anidata',
    schedule=None,           # déclenchement manuel uniquement
    start_date=datetime(2026, 3, 23),
    catchup=False,           # ne pas rejouer les runs passés
) as dag:
```

**Structure du DAG :**

```
say_hello → check_data → data_ready  → end
                      ↘ data_missing ↗
```

- `say_hello` : affiche un message de bienvenue avec le run ID et la date
- `check_data` : vérifie si les fichiers CSV existent → **BranchPythonOperator** qui choisit le chemin
- `data_ready` ou `data_missing` : affiche le résultat selon la branche prise
- `end` : tâche finale avec `trigger_rule='none_failed_min_one_success'` pour accepter les deux branches

Le `BranchPythonOperator` est la notion clé ici : il retourne le `task_id` de la prochaine tâche à exécuter selon une condition. Les autres branches sont automatiquement **skippées**.

### Le DAG `extract_anime`

Le deuxième DAG est fonctionnel : il charge et valide les 3 fichiers CSV.

**Structure avec tâches parallèles :**

```
extract_anime_csv ↘
extract_ratings_csv → validate_extraction
extract_synopsis_csv ↗
```

Les 3 extractions tournent **en parallèle** (gain de temps). La validation attend que les 3 soient terminées avant de vérifier les résultats via XCom.

```python
# Dépendances définies à la fin du DAG
[t_anime, t_ratings, t_synopsis] >> t_validate
```

**Gestion des erreurs et retries :**

```python
default_args = {
    'retries': 2,                          # 2 tentatives en cas d'échec
    'retry_delay': timedelta(minutes=2),   # attendre 2 min entre chaque retry
}
```

Si une tâche échoue (fichier manquant, erreur réseau...), Airflow la relance automatiquement 2 fois avant de marquer le DAG en échec.

### Résultat

Les DAGs sont tous détectés par Airflow et apparaissent dans l'UI :

```
anomaly_detector   | anomaly_detector.py   | sakura-analytics
etl_pipeline       | etl_pipeline.py       | sakura-analytics
extract_anime      | extract_anime.py      | sakura-analytics
hello_anidata      | hello_anidata.py      | sakura-analytics
load_elasticsearch | load_elasticsearch.py | sakura-analytics
transform_anime    | transform_anime.py    | sakura-analytics
```

Le DAG `hello_anidata` a été déclenché manuellement et s'est exécuté avec succès en moins de 5 secondes.

### Commandes utiles

```bash
make airflow      # Lancer Airflow (webserver + scheduler)
make stop-airflow # Arrêter Airflow
make logs-airflow # Voir les logs du scheduler en temps réel
```

Depuis l'UI (`http://localhost:8080`, admin / admin123) :
- Voir tous les DAGs et leur statut
- Déclencher un DAG manuellement avec le bouton ▶
- Voir le graphe des tâches et leurs logs

---

---

## Séance 5 — Pipeline Extract : lecture multi-fichiers & validation

### Ce que j'ai fait

J'ai lancé et validé le DAG `extract_anime` qui extrait les 3 fichiers CSV en parallèle et vérifie leur schéma.

### DAG multi-tasks avec dépendances

L'idée clé de cette séance est l'**extraction parallèle** : au lieu de lire les fichiers un par un (lent), on les lit tous en même temps grâce à la structure du DAG :

```
extract_anime_csv   ↘
extract_ratings_csv → validate_extraction
extract_synopsis_csv ↗
```

En Airflow, on déclare le parallélisme simplement avec des crochets :
```python
[t_anime, t_ratings, t_synopsis] >> t_validate
```
Airflow comprend que les 3 tâches peuvent tourner en même temps et que `validate_extraction` doit attendre qu'elles soient toutes terminées.

### Validation de schéma

Avant de continuer le pipeline, on vérifie que les fichiers ont bien les colonnes attendues :

```python
def _validate_schema(df, required_cols, name):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"[{name}] Colonnes manquantes : {missing}")
```

Si une colonne obligatoire manque (par exemple si le dataset a changé), la tâche lève une exception et Airflow marque le DAG en **failed** — ce qui empêche les étapes suivantes de tourner sur des données incorrectes.

### Communication entre tâches via XCom

Chaque tâche d'extraction pousse la forme du DataFrame (nombre de lignes × colonnes) dans les XComs :

```python
context['ti'].xcom_push(key='anime_shape', value=list(df.shape))
```

La tâche de validation les récupère pour afficher un rapport :
```python
anime_shape = ti.xcom_pull(task_ids='extract_anime_csv', key='anime_shape')
# → [17562, 35]  (17562 lignes, 35 colonnes)
```

### Gestion des retries

```python
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}
```

Si une lecture de fichier échoue (disque occupé, fichier verrouillé...), Airflow attend 2 minutes et réessaie automatiquement 2 fois avant de considérer la tâche comme échouée.

### Résultat

DAG `extract_anime` exécuté avec **succès** — les 3 fichiers CSV ont été lus et validés.

---

## Séance 6 — Pipeline Transform : nettoyage & feature engineering automatisés

### Ce que j'ai fait

J'ai lancé le DAG `transform_anime` qui réutilise le code de nettoyage des séances 1 et 2, mais cette fois **automatisé et orchestré par Airflow**.

### Modularisation du code

Au lieu d'avoir tout le code dans le DAG, j'ai créé des modules Python réutilisables dans `src/transform/transform_anime.py`. Le DAG importe et appelle ces fonctions :

```python
from transform.transform_anime import clean_anime, enrich_anime
```

C'est une bonne pratique car :
- Le même code peut être testé indépendamment d'Airflow
- On peut l'importer depuis n'importe quel autre DAG
- Le DAG reste lisible (il orchestre, il ne fait pas le travail)

### BranchPythonOperator — logique conditionnelle

La nouveauté de cette séance est le **branchement conditionnel** : selon la qualité des données, le pipeline prend un chemin différent.

```python
def check_data_quality(**context):
    missing_score_pct = df['Score'].isna().mean()
    if missing_score_pct > 0.3:
        return 'clean_heavy'   # Nettoyage renforcé si > 30% de scores manquants
    return 'clean_standard'    # Nettoyage standard sinon
```

**Structure du DAG :**
```
load_raw → check_data_quality → clean_standard → enrich → export_gold
                              ↘ clean_heavy    ↗
```

Le `BranchPythonOperator` retourne le `task_id` de la prochaine tâche à exécuter. L'autre branche est automatiquement **skippée** (elle apparaît en violet dans l'UI Airflow).

### Versioning du dataset

À chaque exécution, le dataset gold est exporté avec un timestamp dans son nom :

```python
version = context['execution_date'].strftime('%Y%m%d_%H%M')
# → anime_gold_20260325_1036.csv
```

Cela permet de conserver un historique des versions et de pouvoir revenir en arrière si une transformation introduit un bug.

### Résultat

DAG `transform_anime` exécuté avec **succès** — le dataset gold nettoyé et enrichi a été généré dans `data/gold/`.

---

## Séance 7 — Pipeline Load : indexation Elasticsearch & monitoring

### Ce que j'ai fait

J'ai lancé le DAG `load_elasticsearch` qui indexe automatiquement le dataset gold dans Elasticsearch, puis vérifie que l'indexation s'est bien passée.

### Connexion Airflow → Elasticsearch

Le module `src/load/load_elasticsearch.py` gère la connexion et l'indexation :

```python
def get_es_client():
    es = Elasticsearch(ES_HOST)
    if not es.ping():
        raise ConnectionError(f"Impossible de joindre Elasticsearch à {ES_HOST}")
    return es
```

On teste la connexion avant de commencer — si Elasticsearch est down, la tâche échoue proprement avec un message clair au lieu d'une erreur cryptique.

### Indexation en bulk

Au lieu d'envoyer 17 562 documents un par un (très lent), on utilise l'**API bulk** d'Elasticsearch qui envoie des lots de documents en une seule requête HTTP :

```python
success, errors = helpers.bulk(es, generate_docs(df), raise_on_error=False)
```

C'est beaucoup plus rapide et c'est la méthode recommandée pour les gros volumes.

Chaque document est identifié par son `MAL_ID` comme clé unique (`document_id`). Si on réindexe, les documents existants sont **mis à jour** au lieu d'être dupliqués — c'est ce qu'on appelle l'**idempotence** : on peut relancer le pipeline autant de fois qu'on veut sans créer de doublons.

### Vérification post-indexation

Après le chargement, une tâche dédiée vérifie que tout s'est bien passé :

```python
def verify_indexation(**context):
    es.indices.refresh(index='anime-gold')  # forcer la mise à jour de l'index
    count = es.count(index='anime-gold')['count']
    if count < 1000:
        raise ValueError(f"Trop peu de documents indexés : {count}")
```

On rafraîchit l'index (Elasticsearch indexe en asynchrone) puis on compte les documents. Si le nombre est anormalement bas, la tâche échoue et alerte.

### Test de recherche full-text

En fin de pipeline, on exécute une requête de test pour vérifier que la recherche fonctionne :

```python
result = es.search(
    index='anime-gold',
    body={
        "query": {"match": {"synopsis": "friendship adventure"}},
        "sort": [{"weighted_score": {"order": "desc"}}],
        "size": 5
    }
)
```

Cette requête cherche les animes dont le synopsis contient les mots "friendship" et "adventure", triés par score pondéré.

### Résultat

DAG `load_elasticsearch` exécuté avec **succès** — **17 562 documents** indexés dans Elasticsearch.

```bash
curl http://localhost:9200/anime-gold/_count
# → {"count": 17562}
```

---

## Séance 8 — Pipeline avancé : détection d'anomalies & bonnes pratiques

### Ce que j'ai fait

J'ai lancé le DAG `anomaly_detector` qui analyse les données à la recherche de comportements suspects : spam de notes, scores artificiellement gonflés, taux d'abandon anormaux.

### Pourquoi détecter les anomalies ?

Les données de MyAnimeList sont générées par des millions d'utilisateurs, ce qui signifie qu'il peut y avoir :
- Des **bots** qui notent massivement pour gonfler le score d'un anime
- Des **trolls** qui mettent systématiquement 1/10 pour faire baisser un score
- Des animes avec un score élevé mais avec très peu de votes (non représentatif)

Un pipeline de données propre doit identifier et signaler ces anomalies.

### Règles métier implémentées

**Règle 1 — Spam de ratings :**
Un utilisateur avec plus de 1000 ratings est suspect (difficile d'avoir regardé autant d'animes et de tous les noter).

```python
user_counts = df.groupby('user_id')['rating'].count()
spam_users = user_counts[user_counts > 1000]
```

**Règle 2 — Variance nulle :**
Un utilisateur qui donne toujours exactement la même note (écart-type = 0) est probablement un bot.

```python
user_rating_std = df.groupby('user_id')['rating'].std()
zero_std_users = user_rating_std[user_rating_std == 0]
```

**Règle 3 — Score élevé avec peu de votes :**
Un anime noté 9.0+ avec moins de 100 votes n'est pas fiable statistiquement. C'est pour ça qu'on a créé le `weighted_score` en séance 2.

```python
suspicious = df[(df['Score'] >= 9.0) & (df['Scored By'] < 100)]
```

### Structure parallèle du DAG

```
detect_rating_spam   ↘
                      → generate_anomaly_report → index_anomalies
detect_score_anomalies ↗
```

Les deux détections tournent en parallèle (données indépendantes), puis le rapport est généré quand les deux ont fini.

### Indexation du rapport dans Elasticsearch

Le rapport d'anomalies est lui-même indexé dans un index dédié `anidata-anomalies` :

```python
es.index(index='anidata-anomalies', body={
    'timestamp': datetime.utcnow().isoformat(),
    'spam_users_count': ...,
    'suspicious_high_score': ...,
})
```

Cela permet de suivre l'évolution des anomalies dans le temps et de les visualiser dans Grafana.

### Scheduling et bonnes pratiques

Le DAG tourne tous les jours à 3h du matin (après le pipeline ETL qui tourne à 2h) :

```python
schedule='0 3 * * *'
```

**Idempotence** : si le DAG est relancé plusieurs fois sur la même journée, les résultats sont identifiés par timestamp et ne s'écrasent pas.

**Backfill** : Airflow peut rejouer les runs passés si on a ajouté de nouvelles règles de détection. `catchup=False` désactive ce comportement par défaut pour éviter une avalanche de runs.

### Résultat

DAG `anomaly_detector` exécuté avec **succès** — le rapport d'anomalies a été généré et indexé dans Elasticsearch.

---

---

## Séance 9 — Soutenance & Démo finale

### Ce que j'ai fait

Pour la soutenance, j'ai assemblé le pipeline complet et créé les dashboards Grafana interactifs qui se mettent à jour automatiquement après chaque run Airflow.

### Pipeline ETL complet en un clic

Le DAG `etl_pipeline` orchestre les 3 DAGs principaux dans l'ordre via `TriggerDagRunOperator` :

```
pipeline_start → trigger_extract → trigger_transform → trigger_load → pipeline_end
```

```python
t_extract = TriggerDagRunOperator(
    task_id='trigger_extract',
    trigger_dag_id='extract_anime',
    wait_for_completion=True,   # attendre que extract_anime soit terminé
    poke_interval=30,           # vérifier toutes les 30 secondes
)
```

`wait_for_completion=True` est essentiel : sans ça, le DAG parent déclencherait les sous-DAGs sans attendre leur résultat. Chaque étape attend la précédente avant de lancer la suivante.

Le pipeline est planifié tous les jours à 2h du matin :
```python
schedule='0 2 * * *'
```

**Résultat du run :** pipeline complet exécuté en **1 minute 30 secondes**.

### Dashboards Grafana

J'ai créé un dashboard interactif (`grafana/dashboards/anidata_dashboard.json`) avec 8 visualisations :

| Panel | Type | Ce qu'il montre |
|-------|------|-----------------|
| Total animes indexés | Stat | Nombre total de documents dans Elasticsearch |
| Score moyen global | Stat | Moyenne du score sur tous les animes |
| Top 10 Genres | Bar chart | Les genres les plus représentés |
| Top 10 Studios | Bar chart | Les studios avec le plus d'animes |
| Répartition par Type | Donut | TV vs Movie vs OVA vs ONA... |
| Distribution des scores | Histogramme | Comment les scores se répartissent |
| Grand Studio vs Indépendant | Pie chart | Répartition par catégorie de studio |
| Top 20 animes | Tableau | Meilleurs animes triés par weighted_score |

### Provisioning automatique

Pour que Grafana charge automatiquement la datasource et le dashboard au démarrage (sans configuration manuelle), j'ai utilisé le **provisioning** :

- `grafana/provisioning/datasources/elasticsearch.yml` → configure la connexion à Elasticsearch
- `grafana/dashboards/dashboard.yml` → indique à Grafana où trouver les fichiers JSON de dashboards
- `grafana/dashboards/anidata_dashboard.json` → le dashboard lui-même

Ces fichiers sont montés dans le container via les volumes Docker :
```yaml
volumes:
  - ../../grafana/dashboards:/etc/grafana/provisioning/dashboards
  - ../../grafana/provisioning/datasources:/etc/grafana/provisioning/datasources
```

### Architecture finale du projet

```
                    ┌─────────────────────────────────────┐
                    │         Apache Airflow               │
                    │  ┌──────────────────────────────┐   │
                    │  │  etl_pipeline (chaque jour 2h)│   │
                    │  │  start → Extract → Transform  │   │
                    │  │        → Load → end           │   │
                    │  └──────────────────────────────┘   │
                    │  + anomaly_detector (chaque jour 3h) │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │         Elasticsearch                │
                    │    Index: anime-gold (17 562 docs)   │
                    │    Index: anidata-anomalies          │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │            Grafana                   │
                    │   Dashboard interactif (8 panels)    │
                    │   Refresh automatique toutes 30s     │
                    └─────────────────────────────────────┘
```

### Services accessibles

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin123 |
| Elasticsearch | http://localhost:9200 | — |
| Grafana | http://localhost:3000 | admin / anidata123 |

### Commandes pour la démo

```bash
# Lancer tout le projet
make elk
make airflow

# Déclencher le pipeline ETL en live (pour la démo)
docker exec airflow-airflow-scheduler-1 airflow dags trigger etl_pipeline

# Vérifier le nombre de documents indexés
curl http://localhost:9200/anime-gold/_count

# Voir l'état des containers
make status
```

### Bilan des compétences acquises

**Data Refinement**
- Audit de qualité d'un dataset brut (valeurs manquantes, types, doublons)
- Nettoyage et normalisation avec Pandas
- Feature engineering : score bayésien, taux de complétion, classification

**Big Data — ELK**
- Déploiement d'une stack Elasticsearch/Logstash/Grafana via Docker
- Création de mappings Elasticsearch adaptés au modèle de données
- Construction de dashboards interactifs avec Grafana

**ETL & Pipelines — Apache Airflow**
- Conception et implémentation de DAGs (Extract, Transform, Load)
- Orchestration avec dépendances, branchements et parallélisme
- Gestion des erreurs, retries et notifications
- Connexion Airflow → Elasticsearch
- Bonnes pratiques : idempotence, scheduling, monitoring, versioning

---

## Semaine 2 — DevOps & CI/CD

### Contexte

Après avoir construit le pipeline ETL de la semaine 1, l'objectif est de passer du prototype à la production : versionner, tester, conteneuriser et déployer automatiquement l'ensemble de la chaîne.

Un nouveau maillon est ajouté en amont : un **scraper** qui enrichit en continu la base Elasticsearch avec les nouveautés du catalogue anime.

---

### Architecture cible

```
Dev (VS Code)
     │  git push
     ▼
GitHub (repo : Alibenaqa/anidata_lab)
     │  déclenche automatiquement
     ▼
GitHub Actions (CI/CD)
     ├── Lint        → ruff check src/ tests/
     ├── Tests       → pytest (Python 3.10 & 3.11, 14 tests, 80% couverture)
     └── Build/Push  → image Docker publiée sur GHCR
                              │
                              ▼
           ghcr.io/alibenaqa/anidata_lab-airflow:latest
                              │
                              ▼
     Docker Compose (local)
     ├── mock-site      (nginx — site cible du scraper)  :8088
     ├── airflow-scheduler
     ├── airflow-webserver                               :8080
     └── postgres       (métadonnées Airflow)
                              │
                DAG scraper_dag (schedule: @daily)
                     │  scrape mock-site avec BeautifulSoup
                     │  → /opt/airflow/data/raw/anime_YYYYMMDD.json
                     │  TriggerDagRunOperator
                     ▼
                DAG etl_dag
                     │  lecture JSON → indexation bulk
                     ▼
     Elasticsearch (stack ELK semaine 1)          :9200
     Index : anidex-animes (103 documents)
                     │
                     ▼
     Grafana (dashboards semaine 1, auto-refresh)  :3000
```

---

### Nouveaux fichiers — Semaine 2

```
anidata-lab/
├── src/
│   └── anidata_scraper/       ← package scraper (BeautifulSoup)
│       ├── __init__.py
│       └── scraper.py
├── dags/
│   ├── scraper_dag.py         ← scrape le mock-site, écrit JSON
│   └── etl_dag.py             ← lit JSON, indexe dans Elasticsearch
├── mock-site/                 ← site nginx statique (103 animes)
├── tests/
│   ├── fixtures.py            ← HTML de test (sans réseau)
│   └── test_scraper.py        ← 14 tests unitaires
├── docker/airflow/
│   └── Dockerfile             ← image custom Airflow 2.10.4 + scraper
├── .github/workflows/
│   └── ci-cd.yml              ← lint → tests → build → push GHCR
└── pyproject.toml             ← config pytest + ruff
```

---

### Installation & Lancement

#### Prérequis

- Docker Desktop installé et lancé
- Compte GitHub avec accès au repo

#### 1. Cloner le repo

```bash
git clone https://github.com/Alibenaqa/anidata_lab.git
cd anidata_lab
```

#### 2. Lancer la stack ELK (semaine 1)

```bash
docker compose -f docker/elk/docker-compose.yml up -d
```

Elasticsearch disponible sur http://localhost:9200
Grafana disponible sur http://localhost:3000 (`admin` / `anidata123`)

#### 3. Builder et lancer Airflow + mock-site

```bash
docker compose -f docker/airflow/docker-compose.yml build
docker compose -f docker/airflow/docker-compose.yml up -d
```

Airflow disponible sur http://localhost:8080 (`admin` / `admin123`)
Mock-site disponible sur http://localhost:8088

#### 4. Déclencher le pipeline manuellement

```bash
docker exec airflow-airflow-scheduler-1 airflow dags trigger scraper_dag
```

Le `scraper_dag` scrape le mock-site, produit un fichier JSON, puis déclenche automatiquement le `etl_dag` qui indexe les 103 animes dans Elasticsearch.

#### 5. Vérifier le résultat

```bash
curl http://localhost:9200/anidex-animes/_count
# → {"count": 103}
```

---

### CI/CD — GitHub Actions

Le workflow `.github/workflows/ci-cd.yml` se déclenche à chaque push sur `main` :

| Job | Détail |
|-----|--------|
| **Lint** | `ruff check src/anidata_scraper/ tests/` |
| **Tests** | `pytest` sur Python 3.10 et 3.11 — 14 tests, 80% de couverture |
| **Build & Push** | Build du Dockerfile et push sur GHCR (uniquement sur `main`) |

L'image publiée : `ghcr.io/alibenaqa/anidata_lab-airflow:latest`

---

### Services — Récapitulatif complet

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin123 |
| Mock-site | http://localhost:8088 | — |
| Elasticsearch | http://localhost:9200 | — |
| Grafana | http://localhost:3000 | admin / anidata123 |
