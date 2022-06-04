import os
import pandas as pd
import pathlib
import requests
import json
import xmltodict

from datetime import datetime
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.models import Variable
from requests.exceptions import HTTPError
from pathlib import Path
from json import dumps
from airflow import AirflowException
from kafka import KafkaProducer


DAG_NAME = os.path.basename(__file__).replace(".py", "")  # Le nom du DAG est le nom du fichier
TRANSILIEN_TOKEN = Variable.get("TRANSILIEN_TOKEN")

default_args = {
    'owner': 'noobzik',
    'retries': 1,
    'retry_delay': timedelta(seconds=60)
}
current_dir = pathlib.Path.cwd()
print(current_dir)


@dag(DAG_NAME, default_args=default_args, schedule_interval="* 0,1,5-23 * * *", start_date=days_ago(1))
def fetch_data_rer_b():
    """
    Ce DAG est permet de récupérer les prochains départs de l'ensemble de la partie SNCF du RER B
    """
    # Charge les données depuis S3
    @task
    def send_to_kafka(data: pd.DataFrame):
        """
        Fonction qui va envoyer vers un kafka le résultat du prétraitement pour pouvoir
        être consommé par les applications
        """

        brokers = 'localhost:9092'
        topic = 'rer-b-injector'
        producer = KafkaProducer(bootstrap_servers=[brokers], value_serializer=lambda x: dumps(x).encode('utf-8'))

        producer.send(topic, value=data.to_json())
    
    @task()
    def fetch_real_time_data():
        """
        Fetch all data related to SNCF RER B stations and process it as a Dataframe.
        Sends the processed data to a Kafka Producer
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

            return df

        headers = {
            'Authorization': 'Basic ' + TRANSILIEN_TOKEN
        }

        df_array: list = []
        url = [
            "https://api.transilien.com/gare/87001479/depart",  # Charles de Gaulles 2
            "https://api.transilien.com/gare/87271460/depart",  # Charles de Gaulles 1
            "https://api.transilien.com/gare/87271486/depart",  # Parc des expositions
            "https://api.transilien.com/gare/87271452/depart",  # Villepinte
            "https://api.transilien.com/gare/87271445/depart",  # Sevran Beaudottes

            "https://api.transilien.com/gare/87271528/depart",  # Mitry Clay
            "https://api.transilien.com/gare/87271510/depart",  # Villeparisis Mitry-le-Neuf
            "https://api.transilien.com/gare/87271437/depart",  # Vert Galant
            "https://api.transilien.com/gare/87271429/depart",  # Sevran Livry

            "https://api.transilien.com/gare/87271411/depart",  # Aulnay Sous bois
            "https://api.transilien.com/gare/87271478/depart",  # Le Blanc Mesnil
            "https://api.transilien.com/gare/87271403/depart",  # Drancy
            "https://api.transilien.com/gare/87271395/depart",  # Le Bourget
            "https://api.transilien.com/gare/87271304/depart",  # La Courneuve - Aubervilliers
            "https://api.transilien.com/gare/87164798/depart",  # La Plaine Stade-de-France
            "https://api.transilien.com/gare/87271007/depart"]  # Paris Gare-du-Nord
        response = []
        for u in url:
            try:
                feed = requests.request("GET", u, headers=headers)
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
        df = pd.concat(df_array)
        pattern = r'^\D'  # Filtre pour récupérer uniquement les codes mission du RER B
        df.reset_index(drop=True, inplace=True)
        try:
            df = df[df['trip_short'].str.contains(pattern)]
        except ValueError:
            df.to_csv("debug_short_trip.csv", index=False, mode='a', header=False)
            print("Error at trip short not containing pattern (No train available maybe)")

        unix_timestamp = datetime.now().timestamp()
        # Getting date and time in local time
        datetime_obj = datetime.fromtimestamp(int(unix_timestamp))
        df = df.assign(timestamp=datetime_obj)
        df.to_csv()
        # Sauvegarder le fichier CSV final
        output_dir = Path('data/processed/' + str(datetime_obj.month) + '/' + str(datetime_obj.day))
        output_file = 'data-reel.csv'
        output_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_dir / output_file, index=False, mode='a', header=False)


    fetch_real_time_data()


dag_projet_instances = fetch_data_rer_b()  # Instanciation du DAG

# Pour run:
#airflow dags backfill --start-date 2019-01-02 --end-date 2019-01-03 --reset-dagruns daily_ml
# airflow tasks test aggregate_data_2 2019-01-02
