from celery import Celery

app = Celery('mucor3_flask.celery_server',backend='rpc://',
    broker='pyamqp://admin:mypass@rabbit//',include=['mucor3_flask.celery_server.tasks'])
