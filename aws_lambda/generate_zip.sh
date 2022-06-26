#bin/bash
for d in */ ; do
  echo "$d"
  cd "$d" || exit
  echo "$PWD"
  pip install --target ./packages -r requirement.txt
  zip -r function.zip packages
  zip -g function.zip lambda_function.py
  cd ".." || exit
done