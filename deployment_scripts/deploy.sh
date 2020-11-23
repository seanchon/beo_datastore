#!/bin/bash

if [[ "$1" =~ ^(dev|staging|prod)$ ]]; then
  environment=$1
else
    echo "Usage: deploy.sh (dev|staging|prod)"
    exit 1
fi

env_name="beo-datastore-${environment}"
cur_date_time=$(date -u +%y.%m.%d__%H:%M);
command=(eb deploy $env_name --label "${env_name}-${cur_date_time}")
echo "Running command: ${command[@]}"
${command[@]}

command=(eb deploy "${env_name}-worker" --label "${env_name}-worker-${cur_date_time}")
echo "Running command: ${command[@]}"
${command[@]}

echo "Tagging commit..."
cur_date=$(date -u +%y.%m.%d-%H,%M);
export TAG_NAME="deployments/$environment/$cur_date";
git tag -a "$TAG_NAME" -m "Deployed to $environment on $cur_date."

echo "Pushing tags..."
git push origin --tags;

echo "Done!"
