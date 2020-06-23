#!/bin/bash

if [[ "$1" =~ ^(dev|staging|prod)$ ]]; then
  environment=$1
else
    echo "Usage: deploy.sh (dev|staging|prod)"
    exit 1
fi

command=(eb deploy beo-datastore-"${environment}")
echo "Running command: ${command[@]}"
${command[@]}

command=(eb deploy beo-datastore-"${environment}"-worker)
echo "Running command: ${command[@]}"
${command[@]}

echo "Creating tag ${environment} in git."
git tag -d "$environment";
git push origin --delete "$environment";
git tag -a "$environment" -m "Deployed to $environment."
git push origin --tags;
