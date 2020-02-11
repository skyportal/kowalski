FROM kowalski_ingester

# run container
#CMD /usr/local/bin/supervisord -n -c supervisord_producer.conf
#CMD python kafka_producer.py
CMD /bin/bash
