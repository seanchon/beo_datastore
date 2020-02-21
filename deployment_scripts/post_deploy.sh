#!/bin/bash
# Actions performed following web server restart

/etc/init.d/celeryd restart
/etc/init.d/celerybeat stop
/etc/init.d/celerybeat start
