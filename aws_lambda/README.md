# Dossier AWS Lambda

En cours de rédaction
@todo

### Cheatsheet : Générer un fichier zippé pour AWS Lambda
```shell
for d in "./*"
do
  pip install --target ./packages -requierement.txt
  cd package
  zip -r function.zip packages
  zip -g function.zip lambda_function_pyspark.py
done
```