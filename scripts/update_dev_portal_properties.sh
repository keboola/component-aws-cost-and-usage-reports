#!/usr/bin/env bash

set -e
# Obtain the component repository and log in
docker pull quay.io/keboola/developer-portal-cli-v2:latest


# Update properties in Keboola Developer Portal
echo "Updating long description"
value=`cat component_config/component_long_description.md`
echo "$value"
if [ ! -z "$value" ]
then
    docker run --rm \
            -e KBC_DEVELOPERPORTAL_USERNAME \
            -e KBC_DEVELOPERPORTAL_PASSWORD \
            quay.io/keboola/developer-portal-cli-v2:latest \
            update-app-property ${KBC_DEVELOPERPORTAL_VENDOR} ${KBC_DEVELOPERPORTAL_APP} longDescription --value="$value"
else
    echo "longDescription is empty!"
    exit 1
fi

echo "Updating config schema"
value=`cat component_config/configSchema.json`
echo "$value"
if [ ! -z "$value" ]
then
    docker run --rm \
            -e KBC_DEVELOPERPORTAL_USERNAME \
            -e KBC_DEVELOPERPORTAL_PASSWORD \
            quay.io/keboola/developer-portal-cli-v2:latest \
            update-app-property ${KBC_DEVELOPERPORTAL_VENDOR} ${KBC_DEVELOPERPORTAL_APP} configurationSchema --value="$value"
else
    echo "configurationSchema is empty!"
fi


echo "Updating config description"

value=`cat component_config/configuration_description.md`
echo "$value"
if [ ! -z "$value" ]
then
    docker run --rm \
            -e KBC_DEVELOPERPORTAL_USERNAME \
            -e KBC_DEVELOPERPORTAL_PASSWORD \
            quay.io/keboola/developer-portal-cli-v2:latest \
            update-app-property ${KBC_DEVELOPERPORTAL_VENDOR} ${KBC_DEVELOPERPORTAL_APP} configurationDescription --value="$value"
else
    echo "configurationDescription is empty!"
fi


echo "Updating short description"

value=`cat component_config/component_short_description.md`
echo "$value"
if [ ! -z "$value" ]
then
    docker run --rm \
            -e KBC_DEVELOPERPORTAL_USERNAME \
            -e KBC_DEVELOPERPORTAL_PASSWORD \
            quay.io/keboola/developer-portal-cli-v2:latest \
            update-app-property ${KBC_DEVELOPERPORTAL_VENDOR} ${KBC_DEVELOPERPORTAL_APP} shortDescription --value="$value"
else
    echo "shortDescription is empty!"
    exit 1
fi