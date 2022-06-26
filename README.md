Regone
==============================

Un projet qui calcule les écarts du temps de parcours d'une gare A vers une gare B de la ligne du RER B Partie SNCF (Gare du Nord &harr; Mitry Claye / Aeroport Charles-de-Gaulles)

Actuellement :
* Calcul du temps de parcours théoriques.
* Récupération de l'information trafic de la ligne B auprès de la RATP.
* Récupération d'un itinéraire de substitution auprès de Ile-de-France Mobilités.
* Récupération des prochains départs.
* Stockage des prochains départs vers un S3 en CSV.
* Stockage de l'offre de transport de la journée vers un S3 en CSV.
* Stockage de la moyenne théorique du temps de parcours vers un S3 en CSV.
* Restitution de l'information à travers des fonctions AWS Lambda.
* Mise à disposition du schéma API de AWS API Gateway.
* Image docker pour déployer airflow et kafka vers les instances EC2.

A venir :
* Script Terraform pour déployer les instances nécessaires
* Topic Kafka pour avoir l'heure d'arrivé en gare
* Job Spark pour constituer un Dataset d'entrainement pour détecter les écarts de parcours (alerte trafic potentiellement perturbé) à partir des données temps réels récoltés dans le topic Kafka
* Modèle d'IA (moving average last 5 trains, from A to B)
* Inférence
* Déclencheur de notification en provenance de Sagemaker pour AWS SNS afin d'envoyer une alerte sur le trafic perturbé.

Les endpoints actuellement déployés :
* Prochains trains (Sans Paramètres)
* Itinéraires de substitution (Gare départ, Gare d'arrivé)
* Temps de parcours théorique (gare départ, gare d'arrivé, direction) 
* Information trafic (Sans Paramètres)

Les endpoints à venir:
* Temps de parcours réel (moyenne 5 derniers trains)

Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── aws_lambda         <- Contains Lambda functions for AWS Lambda
    │   ├── calculate_theory
    │   ├── grab_info_trafic
    │   ├── grab_itineraire
    │   └── grab_next_departures
    │
    ├── docker             <- Contains a docker-compose to quickly deploy into an EC2 instance
    │   ├── airflow        <- Airflow docker image ready to be deploy with a docker-compose
    │   │   ├── dags                    <- Dags to retrieve external data, spark jobs, etc ...
    │   │   ├── Dockerfile              <- Custom build Dockerfile
    │   │   └── docker-composer.yml     <- `docker-compose up`
    │   ├── kafka          <- Airflow docker image ready to be deploy with a docker-compose 
    │   │   └── docker-composer.yml    <- This file needs to be customized for `KAFKA_CFG_LISTENERS`, 
    │                                     `KAFKA_CFG_ADVERTISED_LISTENERS`
    |                                     -> To launch the kafka instance, use `docker-compose up`
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks used for exploration purpose only, these notebooks
    │                         are not used by the project. It helps writting code for AWS Lambda,
    │                         PySpark, Scala Spark, Airflow Dags. Expect junks here.
    │                         
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    └─── src                <- Source code for use in this project.
        ├── __init__.py    <- Makes src a Python module
        │
        ├── data           <- Scripts to download or generate data
        │   └── make_dataset.py
        │
        ├── features       <- Scripts to turn raw data into features for modeling
        │   └── build_features.py
        │
        ├── models         <- Scripts to train models and then use trained models to make
        │   │                 predictions
        │   ├── predict_model.py
        │   └── train_model.py
        │
        └── visualization  <- Scripts to create exploratory and results oriented visualizations
            └── visualize.py
     


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
