# kimp-package
BYZANTIUM SYNOD

# add celery queues

 *[1]* add to up.sh celery workers
```
poetry run celery -A app.ext.byzantium.background.tasks worker -l info -E -Q drapht_q -c 3 --prefetch-multiplier=1
``` 
 *[2]*  add celery app/config/friends.yaml
```
drapht:
    route: app.ext.byzantium.background.tasks
    queue: byzantium_q
```
