# LOADING DATA

To add datasets to the beo_datastore backend, the following scripts can be run.

## Load Serving Entities
To add new CCAs and Utilities to AWS (Dev, Staging, or Prod):
```
 python manage.py runscript scripts.ingest_load_serving_entities --silent
```
To add new CCAs and Utilities to your local development:
```
python scripts/initialize_dev.py --lse
```


## Base Fixtures

Base data has been populated in the codebase and can be loaded using the following script.

```
python manage.py runscript beo_datastore.scripts.load_data
```

Optional data including sample meters and utility rates can be added with the following flag:

```
python manage.py runscript beo_datastore.scripts.load_data --script-args seed
```

OpenEI data can be loaded with the following flag:

```
python manage.py runscript beo_datastore.scripts.load_data --script-args openei
```

## Electricity Load Data

### OpenEI

The following script will prime the database with all OpenEI reference buildings located in a particular state. It reaches out to the OpenEI website and scrapes the site's content.

```
python manage.py runscript load.openei.scripts.ingest_reference_meters --script-args <STATE>
```

* Where STATE is the two-letter abbreviation of a state (ex. CA).

### Item 17

The following script will load Item 17 data (the CSV file will need to be downloaded locally).

```
python manage.py runscript load.customer.scripts.ingest_item_17 --script-args <LSE_NAME> <CSV_FILE>
```

* Where `<LSE_NAME>` is the name of a load serving entity (ex. "MCE Clean Energy").
* Where `<CSV_FILE>` is the location of an Item 17 file to be used for ingestion.

## Cost Data

### Clean Net Short GHG

The following script will ingest GHG lookup tables from the CPUC's [Clean Net Short Calculator Tool](http://www.cpuc.ca.gov/General.aspx?id=6442451195).

```
python manage.py runscript cost.ghg.scripts.ingest_ghg_data
```

### OpenEI Utility Rate Database

The following script will ingest utility rate data from OpenEI's Utility Rate Database.

```
python manage.py runscript cost.utility_rate.scripts.ingest_openei_utility_rates --script-args <UTILITY_NAME> <SOURCE>
```

* Where `<UTILITY_NAME>` is the name of a utility (ex. "Pacific Gas & Electric Co").
* Where `<SOURCE>` (optional) is the location of an OpenEI formatted JSON file.

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
python manage.py runscript cost.procurement.scripts.ingest_system_profiles --script-args <LSE_NAME> <CSV_FILE>
```

* Where `<UTILITY_NAME>` is the name of a utility (ex. "MCE Clean Energy").
* Where `<CSV_FILE>` is the location of a properly-formatted system-load-profile file to be used for ingestion.
