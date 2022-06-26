import json
import pendulum
import boto3
import pandas as pd


from pyspark.sql.functions import *


def lambda_handler(event, context):
    """
    Cette fonction lambda calcule le temps de parcours théorique entre une gare A et une gare B en fcontion de sa
    direction.
    Les fichiers CSV sont stockés dans un bucket s3 et sont traités par Spark.
    Le restultat est restitué dans un objet JSON auprès de celui qui a appelé cette fonction lambda
    """
    gare_depart: str = event['queryStringParameters']['gareDepart']
    gare_arrive: str = event['queryStringParameters']['gareArrive']
    direction: int = int(event['queryStringParameters']['direction'])

    #datetime_obj: pendulum.DateTime = pendulum.now("Europe/Paris")
    s3 = boto3.client("s3")
    S3_BUCKET_NAME = "sncf-rer-b"
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key="data/ref/transilien-gtfs-2022-06-26.csv")
    df_referentiel = pd.read_csv(obj['Body'])

    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key="data/ref/relation_ordre_RER_B.csv")
    df_relation = pd.read_csv(obj['Body'],  header=None, names=["Gare", "Relation", "API"],  delimiter=";")

    #df_referentiel = pd.read_csv('../../docker/airflow/data/processed/gtfs/transilien-gtfs-2022-06-26.csv')
    #df_relation = pd.read_csv('../../docker/airflow/data/reference/relation_ordre_RER_B.csv',  header=None, names=["Gare", "Relation", "API"],  delimiter=";")

    response_object = {}
    response_object['statusCode'] = 200
    response_object['headers'] = {}
    response_object['headers']['Content-Type'] = 'application/json'
    response_object['headers']['Access-Control- Allow-Origin'] = '*'
    response_object['body'] = json.dumps(get_travel_time(df_referentiel,
                                                         df_relation,
                                                         gare_depart,
                                                         gare_arrive,
                                                         direction))


    return response_object


def compute_inter_station(gare_depart, gare_arrive, direction) -> list[str]:
    """
    Génère une liste des gare parcourus a partir d'une gare de départ vers une gare d'arrivé
    """

    b1 = ["87001479", "87271460", "87271486", "87271452", "87271445"]  # Aeroport
    b2 = ["87271528", "87271510", "87271437", "87271429"]  # Mitry
    central = ["87271411", "87271478", "87271403", "87271395", "87271304", "87164798", "87271007"]  # Central

    # Detecter l'appartenance de la gare de départ
    res = []
    # Construction des listes
    if direction == 0:  # Direction Sud
        if gare_depart in b1:
            res = b1 + central
        elif gare_depart in b2:
            res = b2 + central
        elif gare_depart in central:
            res = central
    elif direction == 1:  # Direction nord
        if gare_depart in central:
            central.reverse()
            if gare_arrive in central:
                res = central
            elif gare_arrive in b1:
                b1.reverse()
                res = central + b1
            elif gare_arrive in b2:
                b2.reverse()
                res = central + b2
        elif gare_depart in b1:
            b1.reverse()
            res = b1
        elif gare_depart in b2:
            b2.reverse()
            res = b2

    return res[res.index(gare_depart):res.index(gare_arrive) + 1]


def get_travel_time(df: DataFrame, df_relation: DataFrame, gare_depart: str, gare_arrive: str, direction: int) -> str:
    """
    Job spark pour calculer le temps du trajet théorique d'une gare de départ vers une gare d'arrivé, avec sa direcction
    Renvoi une String affichant la
    """
    gares: list[str] = compute_inter_station(gare_depart, gare_arrive, direction)
    del df[df.columns[0]]
    df_bis = df.groupby(by=["origin", "origin_id", "arrival", "direction"]).agg(travelled_mean=('time_travelled', 'mean')).reset_index()
    print(df_bis.describe())
    df_1 = pd.merge(df_bis, df_relation, left_on='origin', right_on='Gare')
    df_2 = pd.merge(df_1, df_relation, left_on='arrival', right_on='Gare')
    df_2.drop(['Gare_x', 'Gare_y'], axis=1, inplace=True)
    df_2 = df_2.dropna().reset_index(drop=True)
    df_3 = df_2[df_2['direction'] == 1].reset_index(drop=True)
    df_3['API_x'] = df_3['API_x'].astype(int).astype(str)
    df_3['API_y'] = df_3['API_y'].astype(int).astype(str)
    df4 = df_3[df_3['API_x'].isin(gares)]
    df4 = df4[df4['API_y'].isin(gares)]
    time = df4['travelled_mean'].sum()
    dt = pendulum.duration(seconds=time)

    # return str(dt.in_minutes()) + " minutes"
    return str(dt.in_minutes()) + " minutes"

if __name__ == '__main__':
    jsons = """
    {
        "queryStringParameters": {
            "gareDepart": "87271437",
            "gareArrive": "87271007",
            "direction": "0"
        }
    }
        """
    event = json.loads(jsons)
    # print(compute_inter_station("87271437", "87271007", 0))
    print(lambda_handler(event, None))
