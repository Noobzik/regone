import requests
import json
import pendulum


def lambda_handler(event, context):
    """
    Cette fonction récupère auprès de la SNCF, une liste d'itinéraire de substitution à partir d'une gare
    de départ et d'une gare d'arrivée.
    Le restultat est restitué dans un objet JSON auprès de celui qui a appelé cette fonction lambda
    """

    # 1. Parse out query string params
    gareA = 'stop_area:SNCF:' + event['queryStringParameters']['gareDepart']
    gareB = 'stop_area:SNCF:' + event['queryStringParameters']['gareArrive']

    # 1. Construct the body of the response object
    transactionResponse = grab_response(gareA, gareB)
    # 2. Construct http response object
    responseObject = {}
    responseObject['statusCode'] = 200
    responseObject['headers'] = {}
    responseObject['headers']['Content-Type'] = 'application/json'
    responseObject['headers']['Access-Control-Allow-Origin'] = '*'
    responseObject['body'] = json.dumps(transactionResponse)
    # 4. Return the response object return responseObject
    return responseObject


def get_token():
    """
    Pour IDFM PRIM : Récupère un token pour traiter les données des endpoints de l'API
    """
    urlOAuth = 'https://as.api.iledefrance-mobilites.fr/api/oauth/token'
    client_id = ""
    client_secret = ""
    data = dict(
        grant_type='client_credentials',
        scope='read-data',
        client_id=client_id,
        client_secret=client_secret
    )
    response = requests.post(urlOAuth, data=data)
    print(response.json)
    # Vérifier le code retour de la requête
    if response.status_code != 200:
        print('Status: ', response.status_code, 'Erreur sur la requête; fin de programme')
        exit()
    json_data = response.json()
    return json_data['access_token']


def grab_response(gare_a: str, gare_b: str):
    """
    Récupère l'endpoint "" qui liste les itinéraires de substitution de la ligne B partie SNCF
    """
    url = "https://api.sncf.com/v1/coverage/sncf/journeys"
    token = ""
    print(token)
    headers = {
        'Accept-Encoding': 'gzip',
        'Authorization': 'Basic ' + token
    }
    # from_from = "stop_area:SNCF:87271445"
    # to = "stop_area:SNCF:87271411"
    forbidden_uris = "line:SNCF:B"
    full_url = url + "?from=" + gare_a + "&to=" + gare_b + "&forbidden_uris[]=" + forbidden_uris
    print(full_url)
    response = requests.get(full_url, headers=headers)

    if response.status_code != 200:
        print('Status:', response.status_code, 'Erreur de la requête : fin de programme')

    json_data = response.json()
    js = json_data

    di = {}
    for j in range(len(js["journeys"])):
        li = {}
        for i in range(len(js["journeys"][j]["sections"])):
            if "display_informations" in js["journeys"][j]["sections"][i]:
                type = js["journeys"][j]["sections"][i]["display_informations"]["physical_mode"] + \
                       " " + js["journeys"][j]["sections"][i]["display_informations"]["name"] + \
                       " direction : " + js["journeys"][j]["sections"][i]["display_informations"][
                           "direction"] + " " + pendulum.duration(
                    seconds=js["journeys"][j]["sections"][i]["duration"]).in_words(
                    locale="fr") + " départ à : " + pendulum.parse(
                    js["journeys"][j]["sections"][i]["departure_date_time"]).to_time_string()

            else:
                type = js["journeys"][j]["sections"][i]["type"] + " "

            if js["journeys"][j]["sections"][i]["type"] == "waiting":
                li[i] = "Patientez : " + pendulum.duration(
                    seconds=js["journeys"][j]["sections"][i]["duration"]).in_words(locale="fr")
            else:
                li[i] = (js["journeys"][j]["sections"][i]["from"]["name"] + " -> " +
                         js["journeys"][j]["sections"][i]["to"]["name"] + " via : " + type)
        di[j] = li

    return di
