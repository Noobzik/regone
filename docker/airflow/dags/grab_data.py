import json
import os
import pathlib
import boto3
import pandas as pd
import pendulum
import requests
import xmlschema
import re
import logging

from pathlib import Path
from airflow import AirflowException
from botocore.exceptions import ClientError
from airflow.decorators import dag, task
from airflow.models import Variable
from kafka import KafkaProducer
from requests.exceptions import HTTPError

DAG_NAME = os.path.basename(__file__).replace(".py", "")  # Le nom du DAG est le nom du fichier
TRANSILIEN_TOKEN = Variable.get("TRANSILIEN_KEY")
AWS_REGION = Variable.get("AWS_REGION")
BUCKET = "sncf-rer-b"
BROKER = Variable.get("BROKER")

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


@dag(DAG_NAME, default_args=default_args, schedule_interval="*/1 5-23 * * *",
     start_date=pendulum.today('Europe/Paris').add(days=-1), catchup=False)
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
        xsd = """<?xml version="1.0" encoding="UTF-8"?>
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
            <xsd:element name="passages">
                <xsd:complexType mixed="true">
                    <xsd:sequence>
                        <xsd:element name="train" type="trainType" minOccurs="0" maxOccurs="unbounded"/>
                    </xsd:sequence>
                    <xsd:attribute name="gare" type="xsd:string" use="required"/>
                </xsd:complexType>
            </xsd:element>
            <xsd:complexType name="trainType">
                <xsd:all>
                    <xsd:element name="date">
                        <xsd:complexType>
                            <xsd:simpleContent>
                                <xsd:extension base="xsd:string">
                                    <xsd:attribute name="mode" use="required">
                                        <xsd:simpleType>
                                            <xsd:restriction base="xsd:string">
                                                <xsd:enumeration value="R"/>
                                                <xsd:enumeration value="T"/>
                                            </xsd:restriction>
                                        </xsd:simpleType>
                                    </xsd:attribute>
                                </xsd:extension>
                            </xsd:simpleContent>
                        </xsd:complexType>
                    </xsd:element>
                    <xsd:element name="num" maxOccurs="1" type="xsd:string"/>
                    <xsd:element name="miss" maxOccurs="1" type="xsd:string"/>
                    <xsd:element name="term" maxOccurs="1" minOccurs="0" type="xsd:string"/>
                    <xsd:element name="etat" maxOccurs="1" minOccurs="0">
                        <xsd:simpleType>
                            <xsd:restriction base="xsd:string">
                                <xsd:enumeration value="Retardé"/>
                                <xsd:enumeration value="Supprimé"/>
                            </xsd:restriction>
                        </xsd:simpleType>
                    </xsd:element>
                </xsd:all>
            </xsd:complexType>
        </xsd:schema>
        """

        class Train:
            """
            Cette classe définit un train conformément au schéma de l'API Transilien.
            Elle est générique pour tout les trains de ce réseau.
            La ligne A et B étant en double exploitation SNCF/RATP, elle hérite de cette classe
            """

            def __init__(self, num: str, miss: str, date: str, mode: str, term: str, time_recorded: pendulum.DateTime,
                         etat: str = ""):
                self.num = num
                self.miss = miss
                self.date = date
                self.mode = mode
                self.direction: str = ""
                self.term = int(term)
                self.time_recorded: pendulum.DateTime = time_recorded
                self.etat = etat

            def get_num(self):
                """
                Retourne le numéro de la mission du train
                """
                return self.num

            def get_date(self):
                """
                Retourne la date et l'heure du train de départ
                """
                return self.date

            def get_mode(self):
                """
                Retourne le mode de l'horraire de départ.
                R pour Réel
                T pour théorique
                """
                return self.mode

            def get_term(self):
                """
                Retourne l'identifiant du terminus du train
                """
                return self.term

            def get_miss(self):
                """
                Retourne la mission du train
                """
                return self.miss

            def get_etat(self):
                """
                Retourne l'état du train, s'il y existe
                """
                return self.etat

            def get_time_recorded(self):
                return self.time_recorded

            def get_train(self):
                """
                Retourne les caractéristiques d'un train
                """
                return self.time_recorded.to_datetime_string(), self.num, self.miss, self.date, self.mode, self.term, self.direction, self.etat

            def to_dict(self):
                """
                Convertie la class train en un dictionnaire
                """
                a = ["time_recorded", self.time_recorded.to_datetime_string(),
                     "num", self.num,
                     "miss", self.miss,
                     "date", self.date,
                     "mode", self.mode,
                     "term", self.term,
                     "direction", self.direction,
                     "etat", self.etat]
                it = iter(a)
                return dict(zip(it, it))

        class RERB(Train):
            """
            Définit un train de la Ligne B et par extension, la ligne A en raison de leur numéro de mission de type ABCD01
            """

            def __init__(self, num: str, miss: str, date: str, mode: str, term: str, time_recorded: pendulum.DateTime,
                         etat: str = ""):
                Train.__init__(self, num, miss, date, mode, term, time_recorded, etat)
                self.direction = int(int(self.get_num()[-2]) % 2)

        class Station:
            def __init__(self, xml_df: dict, date: pendulum.DateTime):
                assert ("@gare" in xml_df and "train" in xml_df)
                size = len(xml_df['train'])
                d: list[RERB] = []
                _pattern = r'[A-Z]{4}[0-9]{2}'
                if size != 0:
                    for i in xml_df['train']:
                        if re.match(_pattern, i["num"]):
                            if "etat" in i:
                                t = RERB(i["num"], i["miss"], i["date"]["$"], i["date"]["@mode"], i["term"], date,
                                         i["etat"])
                            else:
                                t = RERB(i["num"], i["miss"], i["date"]["$"], i["date"]["@mode"], i["term"], date)
                            d.append(t)

                self.gare = int(xml_df["@gare"])
                self.train = d

            def get_station(self):
                """
                Retourne l'identifiant de la gare
                """
                return self.gare

            def get_train(self):
                """
                Retourne la liste des trains de la stations
                """
                return self.train

        def grab_data(gares: list[str]) -> list[str]:
            """
            Grab the SNCF Transilien Opendata next departures of list 'gares' and stores into a lists of xml responses
            """
            payload = {}
            headers = {
                'Authorization': 'Basic ' + TRANSILIEN_TOKEN
            }
            url = "https://api.transilien.com/gare/"
            response = []
            for u in gares:
                try:
                    feed = requests.request("GET", url + u + '/depart/', headers=headers, data=payload)
                    feed.raise_for_status()
                except HTTPError as http_err:
                    print(f'HTTP error occurred: {http_err}')  # Python 3.6
                    exit(0)
                except Exception as err:
                    print(f'Other error occurred: {err}')  # Python 3.6
                    exit(0)
                else:
                    response.append(feed.content)
            return response

        def xml_to_df(list_xml: list[str], xml_schema: str) -> list[dict]:
            """
            Méthode qui va prendre la liste des XML récuéré auprès de Transilien SNCF, renvoie une liste de dictionnaire.
            Les XML sont validés par un schéma, puis converties en dictionnaire.
            """
            df = []
            schema = xmlschema.XMLSchema(xml_schema)
            for l in list_xml:
                df.append(xmlschema.to_dict(l, schema=schema, preserve_root=False))
            return df

        def df_to_station(df_list: list[dict], date: pendulum.DateTime) -> list[Station]:
            """
            Méthode qui va prendre une liste des dictionnaires pour le convertir en une liste Station.
            Chaque Station va comporter une liste de train. Cette liste de train représente la liste des
            prochains départs
            """
            stations = []
            for l in df_list:
                stations.append(Station(l, date))
            return stations

        def stations_to_json(stations_list: list[Station]) -> list[list[dict]]:
            """
            Méthode qui va convertir chaque stations en liste de liste dictionnaire JSON
            """
            json_lists = []
            for s in stations_list:
                train_list = []
                for t in s.get_train():
                    train_list.append(t.to_dict())
                json_lists.append(train_list)
            return json_lists

        def send_to_kafka(data: list[list[dict]], gares: list[str]):
            """
            Méthode qui va envoyer vers un kafka le résultat du prétraitement pour pouvoir
            être consommé par les applications
            """

            for i in range(len(data)):
                df = data[i]
                topic = 'rer-b-' + str(gares[i])
                producer = KafkaProducer(bootstrap_servers=BROKER)
                producer.send(topic, value=(json.dumps(df, ensure_ascii=False).replace("\'", '"').encode('utf-8')))
                producer.flush()
            print("All data sent to kafka")

        def save_df(data: list[list[dict]], gares: list[str], date: pendulum.DateTime):
            """
            Méthode qui va sauvegarder les données sur un CSV en local
            """
            for i in range(len(data)):
                df = pd.DataFrame.from_dict(data[i])
                output_dir = Path('data/processed/' + str(date.month) + '/' + str(date.day))
                output_file = 'data-reel-' + gares[i] + '.csv'
                path = Path(output_dir / output_file)
                if path.is_file():
                    df.to_csv(path, index=False, mode='a', header=False)
                else:
                    output_dir.mkdir(parents=True, exist_ok=True)
                    df.to_csv(path, index=False, mode='a', header=True)

        def save_to_s3(gares: list[str], date: pendulum.DateTime):
            """
            Méthode qui va sauvegarder les données sur un bucket S3
            """

            s3 = boto3.client("s3")

            for i in range(len(gares)):

                # Bucket S3

                output_dir = Path(
                    'data/processed/' + str(date.month) + '/' + str(date.day))
                output_file = 'data-reel-' + gares[i] + '.csv'
                path = Path(output_dir / output_file)
                s3_path = 'data/processed/' + \
                          str(date.month) + '/' + \
                          str(date.day) + '/' + \
                          str(date.hour) + '/' + \
                          str(date.minute)
                if path.is_file():
                    try:
                        s3.upload_file(str(path), 'sncf-rer-b', s3_path + '/' + output_file)
                    except ClientError as e:
                        logging.error(e)
                        return False
            return True

        gares = [
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
            "87271007"  # Paris Gare-du-Nord
        ]

        datetime_obj: pendulum.DateTime = pendulum.now("Europe/Paris")

        data = grab_data(gares)
        xml = xml_to_df(data, xsd)
        stations = df_to_station(xml, datetime_obj)
        jsons = stations_to_json(stations)
        send_to_kafka(jsons, gares)
        # save_df(jsons, gares, datetime_obj)
        # save_to_s3(gares, datetime_obj)

        # Sauvegarder le fichier CSV final

    fetch_real_time_data()


dag_projet_instances = fetch_data_rer_b()  # Instanciation du DAG

# Pour run:
# airflow dags backfill --start-date 2019-01-02 --end-date 2019-01-03 --reset-dagruns daily_ml
# airflow tasks test aggregate_data_2 2019-01-02
