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
    Récupère l'endpoint "general-message" de la ligne du RER B depuis l'API IDFM PRIM (ne fournis pas les bonnes infos)
    Récupère l'endoint traffic/rers/B pour avoir l'info trafic de la ligne B
    """
    # url = "https://traffic.api.iledefrance-mobilites.fr/v1/tr-messages-it/general-message"  # N'affiche pas si le
    # trafic est fluide
    url = "https://api-ratp.pierre-grimaud.fr/v4/traffic/rers/B"
    # token = get_token()
    # print(token)
    # headers = {
    #     'Accept-Encoding': 'gzip',
    #     'Authorization': 'Bearer ' + token
    # }
    # params = dict(
    #     LineRef='STIF:Line::C01743:'
    # )
    response = requests.get(url)

    if response.status_code != 200:
        print('Status:', response.status_code, 'Erreur de la requête : fin de programme')
        exit()
    json_data = response.json()
    data = json_data['result']
    if data['slug'] == 'normal_trav' or data['slug'] == 'normal':
        print("Le trafic est normal : ", data['title'], data['message'])
        return data
    else:
        print("Le trafic n'est pas normal :", data['title'] + data['message'])
        return data


if __name__ == '__main__':
    main()
