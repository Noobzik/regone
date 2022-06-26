import logging
import os
import warnings

import boto3
import pendulum
import zipfile
import pandas as pd
from pathlib import Path
from urllib.request import urlretrieve
from airflow.decorators import dag, task
from airflow.models import Variable
from io import StringIO

from botocore.exceptions import ClientError

DAG_NAME = os.path.basename(__file__).replace(".py", "")  # Le nom du DAG est le nom du fichier
TRANSILIEN_TOKEN = Variable.get("TRANSILIEN_KEY")
AWS_REGION = Variable.get("AWS_REGION")
AWS_ACCESS_ID = Variable.get("AWS_ACCESS_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
BUCKET = "sncf-rer-b"
BROKER = Variable.get("BROKER")

default_args = {
    'owner': 'noobzik',
    'retries': 0,
    'retry_delay': pendulum.duration(minutes=30)
}


@dag(DAG_NAME, default_args=default_args, schedule_interval="0 3 * * *",
     start_date=pendulum.today('Europe/Paris').add(days=-1), catchup=False)
def fetch_gtfs():
    """
    Ce DAG est permet de récupérer les prochains départs de l'ensemble de la partie SNCF du RER B
    """

    # Charge les données depuis S3
    def grab_gtfs(path_where_download):
        """
        Fonction qui va envoyer vers un kafka le résultat du prétraitement pour pouvoir
        être consommé par les applications
        """

        path_gtfs = "https://eu.ftp.opendatasoft.com/sncf/gtfs/transilien-gtfs.zip"
        urlretrieve(path_gtfs, path_where_download)

    @task()
    def process_gtfs(original_file, pf, today_date):
        """
        Fetch all data related to SNCF RER B stations and process it as a Dataframe.
        Sends the processed data to a Kafka Producer
        original_file : location of the zipped gtfs
        pf : location of the processed gtfs
        """
        def import_gtfs(gtfs_path, busiest_date=True):
            """
            Provient de la library gtfs_functions : Charge un fichier gtfs zippé
            """
            try:
                import partridge as ptg
            except ImportError as e:
                os.system('pip install partridge')
                import partridge as ptg

            try:
                import geopandas as gpd
            except ImportError as e:
                os.system('pip install geopandas')
                import geopandas as gpd
            # Partridge to read the feed
            # service_ids = pd.read_csv(gtfs_path + '/trips.txt')['service_id'].unique()
            # service_ids = frozenset(tuple(service_ids))

            if busiest_date:
                service_ids = ptg.read_busiest_date(gtfs_path)[1]
            else:
                with zipfile.ZipFile(gtfs_path) as myzip:
                    myzip.extract("trips.txt")
                service_ids = pd.read_csv('trips.txt')['service_id'].unique()
                service_ids = frozenset(tuple(service_ids))
                os.remove('trips.txt')

            view = {'trips.txt': {'service_id': service_ids}}

            feed = ptg.load_geo_feed(gtfs_path, view)

            routes = feed.routes
            trips = feed.trips
            stop_times = feed.stop_times
            stops = feed.stops
            shapes = feed.shapes

            # Get routes info in trips
            # The GTFS feed might be missing some of the keys, e.g. direction_id or shape_id.
            # To allow processing incomplete GTFS data, we must reindex instead:
            # https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#deprecate-loc-reindex-listlike
            # This will add NaN for any missing columns.
            trips = pd.merge(trips, routes, how='left').reindex(columns=['trip_id', 'route_id',
                                                                         'service_id', 'trip_headsign',
                                                                         'trip_short_name',
                                                                         'direction_id', 'shape_id'])

            # Get trips, routes and stops info in stop_times
            stop_times = pd.merge(stop_times, trips, how='left')
            stop_times = pd.merge(stop_times, stops, how='left')
            # stop_times needs to be geodataframe if we want to do geometry operations
            stop_times = gpd.GeoDataFrame(stop_times, geometry='geometry')

            return routes, stops, stop_times, trips, shapes

        def find_direction(relation: dict, d1: str, d2: str) -> int:
            """
            Cette méthode permet de connaitre la direction de la ligne à partir de deux gares en format str
            Elle renvoie 1 lorsque la direction est nord/est et renvoie 0 lorsque la direction est sud/ouest
            """
            if relation[d1] < relation[d2]:
                return 0  # Sud / Ouest
            else:
                return 1  # Nord / Est

        def define_direction(mission: str) -> int:
            """
            Cette méthode permet de connaitre la direction en fonction du numéro de mission pour le RER A et B
            """
            return int((mission[-2:])) % 2

        def import_relation(path: str) -> dict:
            """
            Cette méthode permet d'importer un fichier contenant une relation d'ordre entre les stations d'une ligne sous la
            forme d'un dictionnaire non ordonné.
            """
            relation = pd.read_csv(path, sep=';',
                                   names=["Station_name", "Station_Order", "Gare_Api"])
            dicto = relation.set_index('Station_name').T.to_dict('index')
            res = dicto["Station_Order"]
            return res

        def save_csv(path_to_save, dataframe: pd.DataFrame):
            """
            Sauvegarde le fichier en csv, préparé à être envoyé sur s3
            """
            dataframe.to_csv(path_to_save, index=False)

        def upload_df_to_s3(path_local, date):

            session = boto3.Session(aws_access_key_id=AWS_ACCESS_ID,
                                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
            s3 = session.client('s3')
            try:
                s3.upload_file(str(path_local), BUCKET, "data/ref/" + "transilien-gtfs-" + date + ".csv")
            except ClientError as e:
                print(logging.error(e))
                exit(-1)



        rer_b_relation = "data/reference/relation_ordre_RER_B.csv"  # Relation d'ordre des gares du RER B
        routes_line = "IDFM:C01743"  # RER B au format IDFM

        # Importation du fichier zippé et de la relation d'ordre
        routes, stops, stop_times, trips, shapes = import_gtfs(original_file)
        relation = import_relation(rer_b_relation)

        # Renomage des colonnes et ajout de time_travelled
        column_names = ["trip_id", "trip_line", "trip_headsign", "trip_short_name", "destination", "destination_id",
                        "origin", "origin_id", "time_departure", "arrival", "arrival_id", "time_arrival",
                        "time_travelled"]
        cleaned_df = pd.DataFrame(columns=column_names)

        rer_b_trips = trips[trips.route_id == routes_line]  # Filtrage pour garder uniquement la ligne B
        rer_b_trips = rer_b_trips.reset_index(drop=True)  # Réinitialisation de l'index du dataframe

        df = stop_times.merge(rer_b_trips,
                              on=['trip_id', 'route_id', 'service_id', 'trip_headsign', 'trip_short_name',
                                  'direction_id',
                                  'shape_id'])  # Jointure des deux fichiers trips.txt et stop_times.txt

        # Calcul du temps d'arret par station
        df["temps_darret"] = df["departure_time"] - df["arrival_time"]

        # Time difference

        a = df["trip_id"].unique()
        # start_time = time.time()

        for journeys in a:
            test = df[df.trip_id == journeys].reset_index(drop=True)
            for i in range(1, len(test)):
                if i == 1:
                    start = test.loc[i - 1, 'stop_name']
                    destination = test.loc[len(test) - 1, 'stop_name']
                    destination_id = test.loc[len(test) - 1, 'parent_station']
                    direction = find_direction(relation, start, destination)

                trip_id = test.loc[i, 'trip_id']
                trip_line = test.loc[i, 'route_id']
                trip_headsign = test.loc[i - 1, "trip_headsign"]
                trip_short_name = test.loc[i - 1, "trip_short_name"]
                origin = test.loc[i - 1, 'stop_name']
                origin_id = test.loc[i - 1, 'parent_station']
                time_departure = test.loc[i - 1, 'departure_time']

                arrival = test.loc[i, 'stop_name']
                arrival_id = test.loc[i, 'parent_station']
                time_arrival = test.loc[i, 'arrival_time']
                time_travelled = time_arrival - time_departure
                warnings.simplefilter(action='ignore', category=FutureWarning)
                cleaned_df = cleaned_df.append({
                    "trip_id": trip_id,
                    "trip_line": trip_line,
                    "trip_headsign": trip_headsign,
                    "trip_short_name": trip_short_name,
                    "destination": destination,
                    "destination_id": destination_id,
                    "origin": origin,
                    "origin_id": origin_id,
                    "time_departure": time_departure,
                    "arrival": arrival,
                    "arrival_id": arrival_id,
                    "time_arrival": time_arrival,
                    "time_travelled": time_travelled,
                    "direction": direction},
                    ignore_index=True)
                warnings.simplefilter(action='default', category=FutureWarning)

        save_csv(pf, cleaned_df)
        upload_df_to_s3(pf, today_date)

        # print("--- %s seconds ---" % (time.time() - start_time))
#        upload_to_s3(cleaned_df)

    today_date = pendulum.now("Europe/Paris").to_date_string()

    path = Path('data/external/') # Chemin de téléchargement du gtfs
    path_processed = Path("data/processed/gtfs/") # Chemin de sauvegarde du gtfs

    file_origin = 'transilien-gtfs-' + today_date + ".zip"
    file_csv = 'transilien-gtfs-' + today_date + ".csv"

    path_file = Path(path / file_origin)
    processed_file = Path(path_processed / file_csv)
    if not path_file.is_dir():
        path.mkdir(parents=True, exist_ok=True)
    if not path_processed.is_dir():
        path_processed.mkdir(parents=True, exist_ok=True)

    grab_gtfs(path_where_download=path_file)
    process_gtfs(path_file, processed_file, today_date)



dag_projet_instances = fetch_gtfs()  # Instanciation du DAG

# Pour run:
# airflow dags backfill --start-date 2019-01-02 --end-date 2019-01-03 --reset-dagruns daily_ml
# airflow tasks test aggregate_data_2 2019-01-02
