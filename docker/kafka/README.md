# Docker : Kafka

Ce docker est à déployer sur une instance EC2 au minimum t2.medium

## Configuration du docker-composer

* Il faut récupérer l'adresse IP local et public de l'instance EC2
* Ouvrir le fichier `docker-compose.yml`
* Modifier la ligne suivante, `INTERNAL` va recevoir l'adresse IP local attribué par AWS et `EXTERNAL`va recevoir 
l'adresse IP public.
```
KAFKA_CFG_ADVERTISED_LISTENERS=INTERNAL://ADRESSE IP LOCAL AWS ICI:19092,EXTERNAL://ADRESSE IP PULIC AWS ICI:9092
```
* Enrengistrer le fichier
* Lancer `docker-compose up`