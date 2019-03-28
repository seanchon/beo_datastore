# BEO Datastore

A place to store data for use in the CEC BEO project.

# SETUP

The following are the steps to get this project up and running. The virtualenv should be built using python3.6.

```
$ virtualenv <env_name>
$ source <env_name>/bin/activate
(<env_name>)$ pip install -r requirements.txt
(<env_name>)$ python manage.py migrate
(<env_name>)$ python manage.py runserver_plus
```

# LOADING DATA

## OpenEI

The following script will prime the database with all OpenEI reference buildings located in California. The script can be modified to allow for all data or different states.

```
(<env_name>)$ python manage.py runscript reference.openei.scripts.ingest_reference_buildings
```

## PG&E

The following script will load PG&E Item 17 data (file will need to be downloaded locally).

```
(<env_name>)$ python manage.py runscript interval.scripts.ingest_pge_data --script-args EXCEL_FILE SHEET_NAME
```

# UPDATING PIP PACKAGES

This project follows the recommended process outlined in https://www.kennethreitz.org/essays/a-better-pip-workflow.

When a new package is added to the project, add it to `requirements-to-freeze.txt` then run `pip install -r requirements-to-freeze.txt` or `pip install -r requirements-to-freeze.txt --upgrade` (if you want to upgrade all packages). Followed by `pip freeze > requirements.txt`.
