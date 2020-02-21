#!/bin/bash

# Copy celery init to the correct location.
-rm /etc/init.d/celeryd
cp /opt/python/ondeck/app/etc/init.d/celeryd /etc/init.d/celeryd
chmod 755 /etc/init.d/celeryd
chown root:root /etc/init.d/celeryd

# Copy celery config to the correct location.
-rm /etc/default/celeryd
cp /opt/python/ondeck/app/etc/default/celeryd /etc/default/celeryd

# Copy celerybeat init to the correct location.
-rm /etc/init.d/celerybeat
cp /opt/python/ondeck/app/etc/init.d/celerybeat /etc/init.d/celerybeat
chmod 755 /etc/init.d/celerybeat
chown root:root /etc/init.d/celerybeat

# Copy celerybeat config to the correct location.
-rm /etc/default/celerybeat
cp /opt/python/ondeck/app/etc/default/celerybeat /etc/default/celerybeat

# Make sure django log files exists
mkdir -p /var/log/django/
touch /var/log/django/django.log
# log files needs to be readable/writable by both wsgi and ec2-user
chmod 666 /var/log/django/*
chown -R wsgi:wsgi /var/log/django/
