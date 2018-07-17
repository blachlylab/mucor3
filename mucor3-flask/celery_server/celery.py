from celery import Celery

app = Celery('celery_server',backend='rpc://',
    broker='pyamqp://admin:mypass@rabbit//',include=['celery_server.tasks'])
