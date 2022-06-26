# Calculate theory

Cas particulier : il faut d'abord déployer le `lambda_function.py` puis déployer un nouveau layer avec le fichier zippé `python.zip`

AWS Lambda demande à ce qu'on importe soi-même les packages qu'on a besoin

* Pandas
* Numpy
* pendulum
* Pytz
* pytzdata

L'approche de départ est de déployer en utilisant pyspark, mais on a pas réussi à le déployer à travers une image docker en raison de l'infracstrure limité de Lambda