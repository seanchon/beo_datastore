# BEO DATASTORE

The backend codebase for NavigaDER. This is a Django application which handles the ingestion of utility meter energy-interval files, runs DER simulations across many meters, and calculates the net impact (ex. customer bills, GHG, procurement, etc.). The corresponding frontend code, which provides a user-interface for this application, is in the [navigader repository](https://github.com/TerraVerdeRenewablePartners/navigader).

## SETUP

The following are the steps to get a development environment up and running for the first time. For instructions on setting up this environment in AWS, see [Deploying BEO Datastore to AWS](docs/AWS.md).

These steps should be performed in the directory where this project has been downloaded. The virtual environment (virtualenv) should be built using python3.6 or later. These steps only need to be performed once.

```
$ python -m venv <env_name>
$ source <env_name>/bin/activate
(<env_name>)$ pip install -r requirements.txt
(<env_name>)$ pre-commit install
(<env_name>)$ brew install redis
(<env_name>)$ pip install redis

```

### SETTING ENVIRONMENT VARIABLES

A handful of environment variables need to be configured on a local machine. On a personal computer, the following should be put into a file called `.env` located in this repository's root directory. The environment variables will be automatically loaded, but the command `export $(<.env)` can be used to export the environment variables to your local machine.

The following can be used with PostgreSQL.

```
APP_ENV=local
APP_URL=http://localhost:3000
BROKER_URL=redis://localhost
CORS_ORIGIN_WHITELIST=http://localhost:3000
DEBUG=1
DJANGO_ALLOWED_HOSTS=localhost
INTERNAL_IPS=127.0.0.1
SECRET_KEY=<SECRET_KEY>
SQL_ENGINE=django.db.backends.postgresql
SQL_DATABASE=<SQL_DATABASE>
SQL_USER=<SQL_USER>
SQL_PASSWORD=<SQL_PASSWORD>
SQL_HOST=localhost
SQL_PORT=5432
```

Additional environment variables will need to be set in AWS Elastic Beanstalk environments (see: [AWS](docs/AWS.md#setting-environment-variables)).

### LAUNCHING THE VIRTUAL ENVIRONMENT

All of the following commands should be run in the virtualenv created in the previous step. The virtualenv can be launched with the following command where `<env_name>` is the name provided in the previous step. After launching the virtualenv, the terminal should show the name of the virtualenv in the terminal prompt.

```
source <env_name>/bin/activate
(<env_name>)$
```

### INITIALIZING DEV ENVIRONMENT

A script is available to initialize a dev environment, which will destroy all existing data and populate data from scratch.

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

### LOADING TEST DATA

For more detailed information about loading test data to a development environment, see [LOADING DATA](docs/LOADING_DATA.md).

### LAUNCHING THE DEVELOPMENT APPLICATION

After each initialization of the dev environment, you will want to create a superuser for site access.

```
python manage.py createsuperuser
```

The `User` will need to be activated and associated with a `LoadServingEntity` in order to run analyses.

The following command can be used to launch the application on your local machine.

```
python manage.py runserver_plus
```

The application can be accessed at http://localhost:8000/ and the administration portal can be accessed at http://localhost:8000/admin/.

### LAUNCHING THE MESSAGE BROKER

The application uses [celery](https://docs.celeryproject.org/en/stable/) to run asynchronous tasks. In order to run celery tasks on a development machine, redis and celery must be launched either in separate windows or as background processes.

```
redis-server
celery worker -A beo_datastore --loglevel=info
```

A custom test-runner has been implemented to run all celery tasks synchronously within tests, which has the impact of better integrity, but slower tests. To enable tests to run asynchronously, the following line can be commented out in `settings.py` while redis and celery are running. This can aid in development speed, however, running tests synchronously should be the final check.

```
TEST_RUNNER = "beo_datastore.libs.test_runner.CeleryTestSuiteRunner"
```

## DEVELOPER NOTES

### CODE FORMATTING

This project is set up to automatically run the [black](https://github.com/psf/black) code-formatting tool as well as running the [flake8](https://pypi.org/project/flake8/) linter. The configuration lives in the codebase, but needs to be initialized in the dev environment with `pre-commit install`. For further details, see [this post](https://ljvmiranda921.github.io/notebook/2018/06/21/precommits-using-black-and-flake8/).

### CODE COVERAGE

Although not enforced, code coverage for Django tests can be viewed by running the following.

```
coverage run manage.py test
coverage report -m  # view in terminal
coverage html -d coverage  # write html report to coverage/
coverage html -d coverage --skip-covered  # ignore files with 100% coverage
```

### UPDATING PIP PACKAGES

This project follows the recommended process outlined in https://www.kennethreitz.org/essays/a-better-pip-workflow.

When a new package is added to the project, add it to `requirements-to-freeze.txt` then run `pip install -r requirements-to-freeze.txt` or `pip install -r requirements-to-freeze.txt --upgrade` (if you want to upgrade all packages). Followed by `pip freeze > requirements.txt`.
