import requests
import json


def lambda_handler(event, context):
    """
    ARN to deploy AWSDataWrangler-Python38
    """
    result = main()
    return {
        'statusCode': 200,
        'body': result
    }


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


def main():
    """
    Récupère l'endpoint "" qui liste les itinéraires de substitution de la ligne B partie SNCF
    """
    url = "https://api.sncf.com/v1/coverage/sncf/journeys"
    token = get_token()
    print(token)
    headers = {
        'Accept-Encoding': 'gzip',
        'Authorization': 'Bearer ' + token
    }
    from_from = "stop_area:SNCF:87271445"
    to = "stop_area:SNCF:87271411"
    forbidden_uris = "line:SNCF:B"
    full_url = url + "?from=" + from_from + "?to=" + to + "?forbidden_uris[]=" + forbidden_uris
    response = requests.get(full_url, headers=headers)

    if response.status_code != 200:
        print('Status:', response.status_code, 'Erreur de la requête : fin de programme')
        exit()
    json_data = response.json()
    data = json_data['result']
    if data['slug'] == 'normal_trav' or data['slug'] == 'normal':
        print("Le trafic est normal : ", data['title'], data['message'])
        return data
    else:
        print("Le trafic n'est pas normale :", data['title'] + data['message'])
        return data