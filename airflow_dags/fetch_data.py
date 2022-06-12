import json
import os
import pathlib
import boto3
import ndjson
import pandas as pd
import pendulum
import requests
import xmltodict

from datetime import datetime
from io import StringIO
from pathlib import Path
from airflow import AirflowException
from airflow.decorators import dag, task
from airflow.models import Variable
from kafka import KafkaProducer
from requests.exceptions import HTTPError

DAG_NAME = os.path.basename(__file__).replace(".py", "")  # Le nom du DAG est le nom du fichier
TRANSILIEN_TOKEN = Variable.get("TRANSILIEN_KEY")
AWS_REGION = Variable.get("AWS_REGION")
BUCKET = "sncf-rer-b"

default_args = {
    'owner': 'noobzik',
    'max_active_runs': 1,
    'retries': 0,
    'depends_on_past': False,
    'catchup_by_default': False,
    'catchup': False,
}
current_dir = pathlib.Path.cwd()
print(current_dir)


@dag(DAG_NAME, default_args=default_args, schedule_interval="*/1 5-23 * * *", start_date=pendulum.today('UTC').add(days=-1))
def fetch_data_rer_b():
    """
    Ce DAG est permet de récupérer les prochains départs de l'ensemble de la partie SNCF du RER B, tout les jours de la
    semaine, de 5h à 23h le jour suivant.
    Attention, sur la partie SNCF, il n'y a plus de train à partir de 22h45 en raison de travaux
    """

    # Charge les données depuis S3

    @task()
    def fetch_real_time_data():
        """
        Fetch all data related to SNCF RER B stations and process it as a Dataframe.
        Sends the processed data to a Kafka Producer and stores to S3
        """

        def make_df(js: dict) -> pd.DataFrame:
            """
            Create a dataframe for the given station
            """
            columns = ['timestamp', 'gare', 'num', 'miss', 'term', 'date.mode', 'date.text', 'direction', 'etat']
            columns_renamed = {"miss": "trip_headsign", "term": "destination", "date.text": "heure_arrive"}
            df = pd.DataFrame.from_dict(js, orient='index')
            try:
                df = df.explode('train').reset_index(drop=True)
                df = df.join(pd.json_normalize(df['train'])).drop('train', axis=1)
                df = df.reindex(columns=columns)
                df = df.rename(columns=columns_renamed)
                df = df.rename(columns={"num": "trip_short"})
                try:
                    df['direction'] = df.apply(lambda row: int((row.trip_short[-2:])) % 2, axis=1)
                except TypeError:
                    # Traiter le cas où il n'y a pas de RER B dans la gare de passage, mais que d'autres train passent
                    print("Error processing direction (maybe its void)")
                    df.to_csv("debug_type_error.csv", index=False, mode='a', header=False)
            except KeyError:
                # Cas ou aucun train n'est présent dans la requête
                columns_renamed = {"miss": "trip_headsign", "term": "destination", "date.text": "heure_arrive"}
                df.to_csv("debug_no_train_to_explode.csv", index=False, mode='a', header=False)
                print("No train value available")

            print("A dataframe is made")

            return df

        def send_to_kafka(data: pd.DataFrame, gare: int):
            """
            Fonction qui va envoyer vers un kafka le résultat du prétraitement pour pouvoir
            être consommé par les applications
            """

            brokers = 'localhost:9092'
            topic = 'rer-b-' + gare
            producer = KafkaProducer(bootstrap_servers=[brokers],
                                     value_serializer=lambda x: json.dump(x).encode("utf-8"))
            producer.send(topic, value=json.loads(ndjson.dump(data.to_dict('records'))))
            producer.close()

        def upload_df_to_s3(dataframe: pd.DataFrame):
            csv_buffer = StringIO()
            dataframe.to_csv(csv_buffer)

            s3_resource = boto3.resource('s3')
            s3_resource.Object(BUCKET, 'airflow/data/df.csv').put(Body=csv_buffer.getvalue())

        headers = {
            'Authorization': 'Basic ' + TRANSILIEN_TOKEN
        }

        df_array: list = []
        api_url = "https://api.transilien.com/gare/"
        url = [
            "87001479",  # Charles de Gaulles 2
            "87271460",  # Charles de Gaulles 1
            "87271486",  # Parc des expositions
            "87271452",  # Villepinte
            "87271445",  # Sevran Beaudottes

            "87271528",  # Mitry Clay
            "87271510",  # Villeparisis Mitry-le-Neuf
            "87271437",  # Vert Galant
            "87271429",  # Sevran Livry

            "87271411",  # Aulnay Sous bois
            "87271478",  # Le Blanc Mesnil
            "87271403",  # Drancy
            "87271395",  # Le Bourget
            "87271304",  # La Courneuve - Aubervilliers
            "87164798",  # La Plaine Stade-de-France
            "87271007"]  # Paris Gare-du-Nord
        response = []
        print(TRANSILIEN_TOKEN)
        for u in url:
            try:
                feed = requests.request("GET", api_url + u + "/depart", headers=headers)
                feed.raise_for_status()
            except HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  # Python 3.6
                raise AirflowException(http_err)
            except Exception as err:
                print(f'Other error occurred: {err}')  # Python 3.6
                raise AirflowException(err)
            else:
                response.append(feed)

        for u in response:
            """ x
            Pré-traitement du JSON et stockage du résultat sous la forme d'un dictionnaire dans une liste de
            dataframe
            """
            as_dict = xmltodict.parse(u.content)
            s = json.dumps(as_dict).replace('\'', '"').replace('#', '').replace('@', '')
            json_object = json.loads(s)
            df_array.append(make_df(json_object))

        pattern = r'^\D'  # Filtre pour récupérer uniquement les codes mission du RER B
        unix_timestamp = datetime.now().timestamp()
        datetime_obj = datetime.fromtimestamp(int(unix_timestamp))
        try:
            for i in range(len(df_array)):
                df = df_array[i]
                df = df[df['trip_short'].str.contains(pattern)]
                df = df.assign(timestamp=datetime_obj)
                output_dir = Path('data/processed/' + str(datetime_obj.month) + '/' + str(datetime_obj.day))
                output_file = 'data-reel-' + url[i] + '.csv'
                output_dir.mkdir(parents=True, exist_ok=True)
                df.to_csv(output_dir / output_file, index=False, mode='a', header=False)
                # if i < 9:
                #    send_to_kafka(df, url[i])
        except ValueError:
            df.to_csv("debug_short_trip.csv", index=False, mode='a', header=False)
            print("Error at trip short not containing pattern (No train available maybe)")

        # Sauvegarder le fichier CSV final

    fetch_real_time_data()


dag_projet_instances = fetch_data_rer_b()  # Instanciation du DAG

# Pour run:
# airflow dags backfill --start-date 2019-01-02 --end-date 2019-01-03 --reset-dagruns daily_ml
# airflow tasks test aggregate_data_2 2019-01-02
