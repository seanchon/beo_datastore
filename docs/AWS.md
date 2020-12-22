# DEPLOYING BEO DATASTORE TO AWS

The main elements that will need to be setup and are necessary to get the backend running in AWS are:

- [Relational Database Service](#relational-database-service) (RDS)
- [Simple Storage Service](#simple-storage-service) (S3)
- [Simple Queue Service](#simple-queue-service) (SQS)
- [Identity and Access Management](#identity-and-access-management) (IAM)
- [Elastic Beanstalk](#elastic-beanstalk) (EB)
- [Elastic Compute Cloud](#elastic-compute-cloud) (EC2) Security Groups
- [Simple Email Service](#simple-email-service) (SES)

## APPLICATION NAME

Choose a meaningful application name that will be used throughout the setup process. The application name is a combination of a string and a `dev`, `staging`, or `prod` identifier. Some recommended application names that have been used in the past are:
* beo-datastore-dev - for development purposes.
* beo-datastore-staging - for staging purposes.
* beo-datastore-prod - for production purposes.

**NOTE**: Throughout this document, the variable name `<application-name>` will be used and the chosen application name should be used in its place.

**ENVIRONMENT VARIABLES**

The following values will be used in configuring the [Elastic Beanstalk Environment Variables](#setting-environment-variables).

* **APP_ENV** - `dev`, `staging`, or `prod` from above.

## RELATIONAL DATABASE SERVICE

A Relational Database Service (RDS) instance needs to be created and the required platform is PostgreSQL. Although Django supports a number of [other databases](https://docs.djangoproject.com/en/3.1/ref/databases/), there are some custom ingest methods that leverage PostgreSQL, so these methods would need to be updated in order to support an alternate database.

The following are some choices available during the creation process and the recommended selection. The defaults should be chosen when a recommendation is not specified.
* Use case: Production, Dev/Test.
  - The Production tier is more expensive and should be used for production workloads, however, the Dev/Test tier is suitable for development and internal use.
* DB Instance Class:
  - db.t3.medium or larger
* DB instance identifier:
  - Use the `<application-name>`.
  - This value will become part of the **SQL_HOST**.
* Master username and Master password:
  - Save this information as **SQL_USER** and **SQL_PASSWORD**.
* Public accessibility:
  - No (recommended)
  - Choosing Yes allows the convenience of external connections, which can be helpful for debugging purposes, but No is more secure.
* Database name:
  - beo_datastore (optional)
  - Save this information as **SQL_DATABASE**.

**ENVIRONMENT VARIABLES**

The following values will be used in configuring the [Elastic Beanstalk Environment Variables](#setting-environment-variables).

* **SQL_ENGINE**: This is `django.db.backends.postgresql` by default.
* **SQL_HOST**: This value is available under the RDS page's **Connectivity & security** -> **Endpoint & port** -> **Endpoint** after the database has been created.
* **SQL_PORT**: This is `5432` by default.
* **SQL_DATABASE**: This is the **Database name** specified above.
* **SQL_USER**: This is the **Master username** specified above.
* **SQL_PASSWORD**: This is the **Master password** specified above.

## SIMPLE STORAGE SERVICE

Two Simple Storage Service (S3) "buckets" need to be created to store:
1. The static assets needed for the backend web interfaces (i.e. HTML, CSS, Javascript).
2. The media assets that the application stores to disk (i.e. CSV and parquet files containing interval data).

The names for each of the two buckets are derived from the `<application-name>`. The two buckets should be called:
1. `<application-name>`-media
2. `<application-name>`-static

**ENVIRONMENT VARIABLES**

The following values will be used in configuring the [Elastic Beanstalk Environment Variables](#setting-environment-variables).

* **AWS_MEDIA_BUCKET_NAME**: `<application-name>`-media.
* **AWS_STORAGE_BUCKET_NAME**: `<application-name>`-static.
* **MEDIA_ROOT**: s3://`<application-name>`-media/
* **STATIC_ROOT**: `<application-name>`.s3-website-us-west-1.amazonaws.com/

## SIMPLE QUEUE SERVICE

One Simple Queue Service (SQS) needs to be created to handle celery tasks.

The following are some choices available during the creation process and the recommended selection. The defaults should be chosen when a recommendation is not specified.
* Name:
  - `<application-name>`

**ENVIRONMENT VARIABLES**

The following values will be used in configuring the [Elastic Beanstalk Environment Variables](#setting-environment-variables).

* **CELERY_DEFAULT_QUEUE**: `<application-name>`

## IDENTITY AND ACCESS MANAGEMENT

One Identity and Access Management (IAM) service account should be created that will allow the Elastic Beanstalk (EB) instances to communicate with the Simple Storage Service (S3) instance and Simple Queue Service (SQS) instance. This is a preventative measure put in place so that only dedicated resources have the ability to access these services.

The following are some choices available during the creation process and the recommended selection. The defaults should be chosen when a recommendation is not specified.

**Add user**
* User name:
  - `<application-name>`
* Access type:
  - Programmatic access

After the service account has been created open **Security credentials** under the account details and choose **Create access key**. Save this information as **AWS_ACCESS_KEY_ID** and **AWS_SECRET_ACCESS_KEY**.

The following policy should be created to grant this service account access to the appropriate resources.

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::<application-name>-static",
                "arn:aws:s3:::<application-name>-static/*",
                "arn:aws:s3:::<application-name>-media",
                "arn:aws:s3:::<application-name>-media/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "sqs:DeleteMessage",
                "sqs:GetQueueUrl",
                "sqs:ChangeMessageVisibility",
                "sqs:DeleteMessageBatch",
                "sqs:SendMessageBatch",
                "sqs:ReceiveMessage",
                "sqs:SendMessage",
                "sqs:GetQueueAttributes",
                "sqs:ChangeMessageVisibilityBatch"
            ],
            "Resource": "arn:aws:sqs:*:<sqs-id>:<application-name>"
        },
        {
            "Effect": "Allow",
            "Action": "sqs:ListQueues",
            "Resource": "*"
        }
    ]
}
```

In order to revoke access to S3, the existing S3 policy should be deleted. The service account policy will grant access to just that account.

In order to revoke access to SQS, the existing SQS policy should be reduced to the following. The service account policy will grant access to just that account.

```
{
  "Version": "2012-10-17",
  "Id": "arn:aws:sqs:us-west-1:<sqs-id>:<sqs-name>/SQSDefaultPolicy"
}
```

**ENVIRONMENT VARIABLES**

The following values will be used in configuring the [Elastic Beanstalk Environment Variables](#setting-environment-variables).

* **AWS_ACCESS_KEY_ID**: This is from **Create access key**.
* **AWS_SECRET_ACCESS_KEY**: This is from **Create access key**.

## ELASTIC BEANSTALK

Two Elastic Beanstalk (EB) instances need to be created. The first EB instance handles all incoming requests and adds tasks to the celery queue. The second EB instance consumes tasks from the celery queue and auto-scales to handle large workloads.

The following are some choices available during the creation process and the recommended selection. The defaults should be chosen when a recommendation is not specified.

**Create a new application**
* Application name:
  - `<application-name>`

**Create a new environment**
* Select environment tier:
  - Web server environment
* Environment-name:
  - `<application-name>`
* Platform:
  - Python

**Create a new environment**
* Select environment tier:
  - Worker environment
* Environment-name:
  - `<application-name>`-worker
* Platform:
  - Python

**ADDITIONAL CONFIGURATION**

Under **Configuration** -> **Capacity**, settings can be modified to allow a greater number of concurrent EC2 instances to exist. This is helpful in the `<application-name>`-worker environment since it can dramatically speed up the computation time for large workloads.

## ELASTIC COMPUTE CLOUD

In order to allow the Elastic Compute Cloud (EC2) instances to communicate with the Relational Database Service (RDS), the Elastic Compute Cloud (EC2) Security Groups need to be configured. The RDS Security Group needs to be configured to have an Inbound Rule allowing the EC2 instances to communicate with it over port 5432.

## SIMPLE EMAIL SERVICE

An email address can be set up with Simple Email Service (SES) or another email provider. This is used for emails sent from the backend - account verification, errors, etc. The application has some additional configuration in `settings.py`.

The following values will be used in configuring the [Elastic Beanstalk Environment Variables](#setting-environment-variables).

* **SMTP_USER**: From a role created by AWS in IAM.
* **SMTP_PASSWORD**: From a role created by AWS in IAM.
* **SUPPORT_EMAIL**: The email user.
* **SUPPORT_PASSWORD**: The email password.


### SETTING ENVIRONMENT VARIABLES

Under **Configuration** -> **Software**, the following **Environment properties** should be carefully set for security reasons and to allow services to communicate with one another.

```
ADMIN_URL=<HARD TO GUESS ADMIN PAGE NAME>
APP_ENV=<FROM APPLICATION NAME>
APP_URL=<PUBLIC URL>
AWS_ACCESS_KEY_ID=<FROM IAM>
AWS_MEDIA_BUCKET_NAME=<FROM S3>
AWS_SECRET_ACCESS_KEY=<FROM IAM>
AWS_STORAGE_BUCKET_NAME=<FROM S3>
CELERY_DEFAULT_QUEUE=<FROM SQS>
CORS_ORIGIN_WHITELIST=<SAME AS APP_URL>
CSRF_COOKIE_SECURE=1
DEBUG=0
MEDIA_ROOT=<FROM S3>
SECRET_KEY=<FROM IAM>
SESSION_COOKIE_SECURE=1
SMTP_PASSWORD=<FROM SES>
SMTP_USER=<FROM SES>
SQL_DATABASE=<FROM RDS>
SQL_ENGINE=django.db.backends.postgresql
SQL_HOST=<FROM RDS>
SQL_PASSWORD=<FROM RDS>
SQL_PORT=5432
SQL_USER=<FROM RDS>
STATIC_ROOT=<FROM S3>
SUPPORT_EMAIL=<FROM SES>
SUPPORT_PASSWORD=<FROM SES>
WORKER=<0 OR 1>
```

As a layer of security the ```ADMIN_URL``` environment variable is provided to change the default Django admin page url to a custom name. This is to prevent attackers to easily profile the backend Django web app.

### DEPLOYING THE APPLICATION TO AWS

The final step of the process is to [deploy the Django application to AWS](https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/create-deploy-python-django.html#python-django-deploy). After the EB CLI is installed, the following commands should be run to deploy the application to both EB instances.

```
eb deploy <application-name>
eb deploy <application-name>-worker
```

The Elastic Beanstalk environment for the web server environment provides a public url which can be used to validate that the application is running.
