# Docker : Airflow

Ce docker est à déployer sur une instance EC2 au minimum t2.medium

## Configuration du docker-composer

1. Ouvrir l'interface web de airflow `aws_adress_ip:8080`
   * Les identifiants pour se connecter sont : `airflow` / `airflow`
2. Aller dans `Admin` &rarr; `Variable`
3. Créer les paramètres suivants

| Paramètres            | Valeur                                  |
|-----------------------|-----------------------------------------|
| AWS_ACCESS_ID         | L'access ID de AWS (IAM)                |
| AWS_SECRET_ACCESS_KEY | La secret access key de AWS (IAM)       |
| AWS_REGION            | La région où S3 et EC2 sont instanciés  |
| BROKER                | L'adresse IP du broker Kafka            |
| TRANSILIEN_KEY        | La clée pour accéder à l'API Transilien |


Un template json suivant permet d'éviter de rentrer à la main les paramètres cités :
```json
{
    "AWS_ACCESS_ID": "",
    "AWS_REGION": "",
    "AWS_SECRET_ACCESS_KEY": "",
    "BROKER": "",
    "TRANSILIEN_KEY": ""
}
```

4. Activez les dags suivants :

| Dags                       | Descriptions                                                                                                                            | Interval  |
|----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|-----------|
| grab_data                  | Dag qui récupère auprès de SNCF Transilien les prochains trains. Le restitue auprès du broker Kafka                                     | 1 minute  |
| grab_gtfs                  | Dag qui récupère, et traite le plan de transport de la journée                                                                          | 24 heures |
| process_parcours_réel      | Dag qui récupère les deux derniers records d'un topic Kafka, et réstitue l'heure d'arrivé du train qui n'est pas dans le dernier record | 2 minutes |
| process_parcours_theorique | Dag qui calcul le temps de parcours théorique du plan de transport de la journée                                                        | 24 heures |

5. Lancez le cluster Kafka
6. Une fois que le cluster est lancé, vérifier qu'il y a pas d'erreur sur l'execution des dags 