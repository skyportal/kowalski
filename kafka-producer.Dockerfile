FROM kowalski_ingester

# copy over the test alerts
COPY data/ztf_alerts/ /data/ztf_alerts/

COPY kowalski/kafka_producer.py /app

# run container
#CMD /usr/local/bin/supervisord -n -c supervisord_producer.conf
#CMD python kafka_producer.py
CMD /bin/bash
