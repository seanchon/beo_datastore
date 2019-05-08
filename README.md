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
(<env_name>)$ python manage.py runscript beo_datastore.scripts.load_data
```

# LAUNCHING THE DEVELOPMENT APPLICATION

When launching the application for the first time, you will want to create a superuser for site access.

```
python manage.py createsuperuser
```

The following command can be used to launch the application on your local machine.

```
(<env_name>)$ python manage.py runserver_plus
```

The application can be accessed at http://localhost:8000/ and the administration portal can be accessed at http://localhost:8000/admin/.

# DEMO NOTEBOOK

To get a feel for the underlying data, the following Jupyter Notebook can be launched. (Note: To expand upon the demo and load additional data, see the next section on [LOADING DATA](#loading-data).)

Enable [RISE](https://github.com/damianavila/RISE) for presentation views.

```
(<env_name>)$ jupyter-nbextension install rise --py --sys-prefix
(<env_name>)$ jupyter-nbextension enable rise --py --sys-prefix
```

Launch a local Jupyter Notebook application.

```
(<env_name>)$ python manage.py shell_plus --notebook
```

After launching the Jupyter Notebook application, navigate to the Notebook application at http://localhost:8888/notebooks/. Opening the demo directory will display some demo notebooks.

# LOADING DATA

To add robust datasets, the following scripts can be run.

## Base Fixtures

Base data has been populated in the codebase and can be loaded using the following script.

```
(<env_name>)$ python manage.py runscript beo_datastore.scripts.load_data
```

## Electricity Load Data

### OpenEI

The following script will prime the database with all OpenEI reference buildings located in a particular state. It reaches out to the OpenEI website and scrapes the site's content.

```
(<env_name>)$ python manage.py runscript load.openei.scripts.ingest_reference_buildings --script-args STATE
```

Where STATE is the two-letter abbreviation of a state (ex. CA).

### PG&E

The following script will load PG&E Item 17 data (the Excel file will need to be downloaded locally).

```
(<env_name>)$ python manage.py runscript load.customer.scripts.ingest_pge_data --script-args EXCEL_FILE SHEET_NAME
```

Where EXCEL_FILE is the location of an Item 17 file and SHEET_NAME is the name of the sheet to be used for ingestion.

## Cost Data

### Clean Net Short GHG

The following script will ingest GHG lookup tables from the CPUC's Clean Net Short Calculator Tool - http://www.cpuc.ca.gov/General.aspx?id=6442451195.

```
(<env_name>)$ python manage.py runscript cost.ghg.scripts.ingest_ghg_data
```

### OpenEI Utility Rate Database

The following script will ingest utility rate data from OpenEI's Utility Rate Database.

```
(<env_name>)$ python manage.py runscript cost.utility_rate.scripts.ingest_openei_utility_rates --script-args UTILITY_NAME (SOURCE)
```

Where UTILITY_NAME is the name of a utility (ex. "Pacific Gas & Electric Co").
Where SOURCE (optional) is the location of an OpenEI formatted JSON file.

To see all possible utilities, run the following command.

```
(<env_name>)$ python manage.py runscript cost.utility_rate.scripts.ingest_openei_utility_rates --script-args help
```

Example: Load data from the OpenEI website.

```
(<env_name>)$ python manage.py runscript cost.utility_rate.scripts.ingest_openei_utility_rates --script-args "Pacific Gas & Electric Co"
```

Example: Load data from a local file.

```
(<env_name>)$ python manage.py runscript cost.utility_rate.scripts.ingest_openei_utility_rates --script-args "MCE Clean Energy" cost/utility_rate/scripts/data/mce_residential_rates_20180501.json
```

# DEVELOPER NOTES

## UPDATING PIP PACKAGES

This project follows the recommended process outlined in https://www.kennethreitz.org/essays/a-better-pip-workflow.

When a new package is added to the project, add it to `requirements-to-freeze.txt` then run `pip install -r requirements-to-freeze.txt` or `pip install -r requirements-to-freeze.txt --upgrade` (if you want to upgrade all packages). Followed by `pip freeze > requirements.txt`.
