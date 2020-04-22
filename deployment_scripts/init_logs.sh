# Make sure django log files exists
mkdir -p /var/log/django/
touch /var/log/django/django.log
# log files needs to be readable/writable by both wsgi and ec2-user
chmod 666 /var/log/django/*
chown -R wsgi:wsgi /var/log/django/
