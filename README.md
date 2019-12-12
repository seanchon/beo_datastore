# BEO Datastore

A place to store data for use in the CEC BEO project.

# SETUP

The following are the steps to get this project up and running for the first time. These steps should be performed in the directory where this project has been downloaded. The virtual environment (virtualenv) should be built using python3.6 or later. These steps only need to be performed once.

```
$ python -m venv <env_name>
$ virtualenv <env_name>
$ source <env_name>/bin/activate
(<env_name>)$ pip install -r requirements.txt
(<env_name>)$ jupyter-nbextension install rise --py --sys-prefix
(<env_name>)$ jupyter-nbextension enable rise --py --sys-prefix
(<env_name>)$ pre-commit install
```

# LAUNCHING THE VIRTUAL ENVIRONMENT

All of the following commands should be run in the virtualenv created in the previous step. The virtualenv can be launched with the following command where <env_name> is the name provided in the previous step. After launching the virtualenv, the terminal should show the name of the virtualenv in the terminal prompt.

```
source <env_name>/bin/activate
(<env_name>)$
```

## INITIALIZING DEV ENVIRONMENT

A script is available to initialize a dev environment. This script can be run anytime to reinitialize the dev environment. (__Note: Any local changes to the code base will be moved into a [git stash](https://git-scm.com/docs/git-stash) and all application data will be destroyed and recreated.__)

```
python scripts/initialize_dev.py --full
```

Optional test data including sample meters and utility rates can be added with the following flag.

```
python scripts/initialize_dev.py --full --test
```

Optional reference building and rate data from OpenEI to be used with demo scripts can be added with the following flag.

```
python scripts/initialize_dev.py --full --demo
```

# LAUNCHING THE DEVELOPMENT APPLICATION

After each initialization of the dev environment, you will want to create a superuser for site access.

```
python manage.py createsuperuser
```

The following command can be used to launch the application on your local machine.

```
python manage.py runserver_plus
```

The application can be accessed at http://localhost:8000/ and the administration portal can be accessed at http://localhost:8000/admin/.

# DEMO NOTEBOOK

To get a feel for the underlying data, a Jupyter Notebook can be launched which contains demo scripts. These scripts can be modified and run against data after it is loaded, which is covered in [LOADING DATA](#loading-data).

```
python manage.py shell_plus --notebook
```

After launching the Jupyter Notebook application, navigate to the Notebook application at http://localhost:8888/notebooks/. Opening the demo directory will display some demo notebooks.

# LOADING DATA

To add robust datasets, the following scripts can be run.

## Base Fixtures

Base data has been populated in the codebase and can be loaded using the following script.

```
python manage.py runscript beo_datastore.scripts.load_data
```

Optional test data including sample meters and utility rates can be added with the following flag.

```
python manage.py runscript beo_datastore.scripts.load_data --script-args test
```

OpenEI data, which is required for some demo notebooks, can be loaded with the following flag.

```
python manage.py runscript beo_datastore.scripts.load_data --script-args demo
```

## Electricity Load Data

### OpenEI

The following script will prime the database with all OpenEI reference buildings located in a particular state. It reaches out to the OpenEI website and scrapes the site's content.

```
python manage.py runscript load.openei.scripts.ingest_reference_buildings --script-args <STATE>
```

* Where STATE is the two-letter abbreviation of a state (ex. CA).

### Item 17

The following script will load Item 17 data (the CSV file will need to be downloaded locally).

```
python manage.py runscript load.customer.scripts.ingest_item_17 --script-args <LSE_NAME> <CSV_FILE>
```

* Where LSE_NAME is the name of a load serving entity (ex. "MCE Clean Energy").
* Where CSV_FILE is the location of an Item 17 file to be used for ingestion.

## Cost Data

### Clean Net Short GHG

The following script will ingest GHG lookup tables from the CPUC's Clean Net Short Calculator Tool - http://www.cpuc.ca.gov/General.aspx?id=6442451195.

```
python manage.py runscript cost.ghg.scripts.ingest_ghg_data
```

### OpenEI Utility Rate Database

The following script will ingest utility rate data from OpenEI's Utility Rate Database.

```
python manage.py runscript cost.utility_rate.scripts.ingest_openei_utility_rates --script-args <UTILITY_NAME> <SOURCE>
```

* Where UTILITY_NAME is the name of a utility (ex. "Pacific Gas & Electric Co").
* Where SOURCE (optional) is the location of an OpenEI formatted JSON file.

To see all possible utilities, run the following command.

```
python manage.py runscript cost.utility_rate.scripts.ingest_openei_utility_rates --script-args help
```

Example: Load data from the OpenEI website.

```
python manage.py runscript cost.utility_rate.scripts.ingest_openei_utility_rates --script-args "Pacific Gas & Electric Co"
```

Example: Load data from a local file.

```
python manage.py runscript cost.utility_rate.scripts.ingest_openei_utility_rates --script-args "MCE Clean Energy" cost/utility_rate/scripts/data/mce_residential_rates_20180501.json
```

### Resource Adequacy Load Curve

The following script will ingest a LSE's load curve, which will be used as the starting point for calculating the net impact of a DER's load impact on RA costs.

```
python manage.py runscript cost.procurement.scripts.ingest_system_profiles --script-args LSE_NAME CSV_FILE
```

* Where UTILITY_NAME is the name of a utility (ex. "MCE Clean Energy").
* Where CSV_FILE is the location of a properly-formatted system-load-profile file to be used for ingestion.

# DEVELOPER NOTES

## CODE FORMATTING

This project is set up to automatically run the black code-formatting tool as well as running the flake8 linter. More details located at https://ljvmiranda921.github.io/notebook/2018/06/21/precommits-using-black-and-flake8/. The configuration lives in the codebase, but needs to be initialized in the dev environment with `pre-commit install`.

## CODE COVERAGE

Although not enforced, code coverage for Django tests can be viewed by running the following.

```
coverage run manage.py test
coverage report -m  # view in terminal
coverage html -d coverage  # write html report to coverage/
coverage html -d coverage --skip-covered  # ignore files with 100% coverage
```

## UPDATING PIP PACKAGES

This project follows the recommended process outlined in https://www.kennethreitz.org/essays/a-better-pip-workflow.

When a new package is added to the project, add it to `requirements-to-freeze.txt` then run `pip install -r requirements-to-freeze.txt` or `pip install -r requirements-to-freeze.txt --upgrade` (if you want to upgrade all packages). Followed by `pip freeze > requirements.txt`.
