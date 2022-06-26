import json
import pendulum
import os

from pyspark.sql import *
from pyspark.sql.types import *
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

    datetime_obj: pendulum.DateTime = pendulum.now("Europe/Paris")

    spark = SparkSession.builder \
        .appName("spark-on-lambda-demo") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .config("spark.hadoop.fs.s3a.session.token", os.environ['AWS_SESSION_TOKEN']) \
        .getOrCreate()

    df_referentiel: DataFrame = spark.read.format("csv") \
        .load("s3://sncf-rer-b/data/ref/transilien-gtfs-" + str(datetime_obj.year) + "-" + str(datetime_obj.month) + "-" + str(datetime_obj.day) + ".csv", header=True)

    relation_schema = StructType() \
        .add("Gare", StringType(), True) \
        .add("Relation", IntegerType(), True) \
        .add("Gare Api", IntegerType(), True)

    df_relation = spark.read.format("csv") \
        .options(delimiter=';') \
        .schema(relation_schema) \
        .load("s3://sncf-rer-b/data/ref/relation_ordre_RER_B.csv", header=False)

    get_travel_time(df_referentiel, df_relation, gare_depart, gare_arrive, direction)

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

    spark.stop()
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
    time = df.groupBy("origin", "origin_id", "arrival", "direction") \
        .agg({'time_travelled': 'avg'}) \
        .filter(df.direction == direction) \
        .join(df_relation, df.arrival == df_relation.Gare) \
        .join(df_relation.select([col(k).alias(k + '_origin') for k in df_relation.columns]).alias('df2_origin'),
              df.origin == col('df2_origin.gare_origin')) \
        .orderBy("Relation_origin") \
        .filter(col("Gare Api_origin").isin(gares)) \
        .filter(col("Gare Api").isin(gares)) \
        .select(col("origin"), col("arrival"), col("direction"), col("avg(time_travelled)")) \
        .agg({'avg(time_travelled)': 'sum'}).collect()[0][0]
    dt = pendulum.duration(seconds=time)

    # return str(dt.in_minutes()) + " minutes"
    return str(dt.in_minutes()) + " minutes"

# if __name__ == '__main__':
#     jsons = """
#     {
#         "queryStringParameters": {
#             "gareDepart": "87271437",
#             "gareArrive": "87271007",
#             "direction": "0"
#         }
#     }
#         """
#     event = json.loads(jsons)
#     # print(compute_inter_station("87271437", "87271007", 0))
#     print(lambda_handler(event, None))
