# BEO Datastore

A place to store data for use in the CEC BEO project.

# SETUP

The following are the steps to get this project up and running. The virtualenv should be built using python3.6.

```
$ python -m venv <env_name>
$ virtualenv <env_name>
$ source <env_name>/bin/activate
(<env_name>)$ pip install -r requirements.txt
(<env_name>)$ python manage.py migrate
(<env_name>)$ python manage.py runserver_plus
```

# LOADING DATA

## Base Fixtures

The following base data can be installed via fixtures. The first is for required reference units and the second is optional to load OpenEI data. Loading OpenEI fixtures can be done in lieu of running the following OpenEI script.

```
(<env_name>)$ python manage.py loaddata reference_unit
(<env_name>)$ python manage.py loaddata openei
```

## OpenEI

The following script will prime the database with all OpenEI reference buildings located in California. It reaches out to the OpenEI website and scrapes the site's content. The script can be modified to allow for all data or different states.

```
(<env_name>)$ python manage.py runscript load.openei.scripts.ingest_reference_buildings
```

## PG&E

The following script will load PG&E Item 17 data (the Excel file will need to be downloaded locally).

```
(<env_name>)$ python manage.py runscript load.customer.scripts.ingest_pge_data --script-args EXCEL_FILE SHEET_NAME
```

# UPDATING PIP PACKAGES

This project follows the recommended process outlined in https://www.kennethreitz.org/essays/a-better-pip-workflow.

When a new package is added to the project, add it to `requirements-to-freeze.txt` then run `pip install -r requirements-to-freeze.txt` or `pip install -r requirements-to-freeze.txt --upgrade` (if you want to upgrade all packages). Followed by `pip freeze > requirements.txt`.
