import csv
import os
import ssl
from pathlib import Path

import pandas
import requests
from requests.exceptions import HTTPError
import pandas as pd
import xmltodict
import json
import time
from json import dumps
from datetime import datetime
from kafka import KafkaProducer

SSL_CONTEXT = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
BROKER_HOST = os.environ.get("BROKER_HOST", "localhost")


def make_df(js: dict) -> pd.DataFrame:
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


def send_to_kafka(data: pandas.DataFrame):
    """
    Fonction qui va envoyer vers un kafka le résultat du prétraitement pour pouvoir
    être consommé par les applications
    """
    # output_dir = Path('../../data/processed/' + str(date_saved.month) + '/' + str(date_saved.day))
    # # output_file = 'data-' + str(date_saved.hour) + '-' + str(date_saved.minute) + '-' + str(date_saved.second) + '.csv'
    # output_file = 'data-reel.csv'

    brokers = 'localhost:9092'
    topic = 'rer-b-injector'
    producer = KafkaProducer(bootstrap_servers=[brokers], value_serializer=lambda x: dumps(x).encode('utf-8'))

    producer.send(topic, value=data.to_json())


def main():
    payload = {}
    headers = {
        'Authorization': 'Basic dG5odG4xMjY0Olc1UXpiM2Q4'
    }

    df_array = []
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
            feed = requests.request("GET", u, headers=headers, data=payload)
            feed.raise_for_status()
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')  # Python 3.6
            return 0
        except Exception as err:
            print(f'Other error occurred: {err}')  # Python 3.6
            return 0
        else:
            response.append(feed)

    for u in response:
        """
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
    output_dir = Path('../../data/processed/' + str(datetime_obj.month) + '/' + str(datetime_obj.day))
    output_file = 'data-reel.csv'
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_dir / output_file, index=False, mode='a', header=False)
    send_to_kafka(df)


if __name__ == "__main__":
    while 1:
        current = datetime.now().timestamp()
        current = datetime.fromtimestamp(int(current))
        print("Grabbing at " + str(current.hour) + '-' + str(current.minute) + '-' + str(current.second))
        main()
        time.sleep(60)
