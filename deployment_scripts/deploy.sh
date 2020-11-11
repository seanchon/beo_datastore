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
cur_date=$(date -u +%d.%m.%y-%H,%M);
export TAG_NAME="deployments/$environment/$cur_date";
git tag -a "$TAG_NAME" -m "Deployed to $environment on $cur_date."
git push origin --tags;
