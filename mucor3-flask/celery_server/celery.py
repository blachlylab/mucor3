from celery import Celery

app = Celery('celery_server',backend='rpc://',
    broker='pyamqp://127.0.0.1//',include=['celery_server.tasks'])
