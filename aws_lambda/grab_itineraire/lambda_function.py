import requests
import json


def lambda_handler(event, context):
    """
    ARN to deploy AWSDataWrangler-Python38
    Grab Itineraire
    """

    # 1. Parse out query string params
    gareDepart = event['queryStringParameters']['gareDepart']
    gareArrive = event['queryStringParameters']['gareArrive']

    # 1. Construct the body of the response object
    transactionResponse = grab_response(gareDepart, gareArrive)
    # 2. Construct http response object
    responseObject = {}
    responseObject['statusCode'] = 200
    responseObject['headers'] = {}
    responseObject['headers']['Content-Type'] = 'application/json'
    responseObject['headers']['Access-Control- Allow-Origin'] = '*'
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
    return json_data
