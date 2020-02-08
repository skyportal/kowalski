FROM python:3.7

RUN apt-get update

# place to keep our app and the data:
RUN mkdir -p /app /data /data/logs /_tmp

# copy over the secrets and the code
COPY secrets.json kowalski/*_api.json kowalski/g*.py kowalski/middlewares.py kowalski/utils.py kowalski/api.py /app/

# change working directory to /app
WORKDIR /app

# install python libs and generate keys
RUN pip install -r /app/requirements_api.txt && python generate_secrets.py

# run tests
#RUN python -m pytest -s api.py

# run container
#CMD /usr/local/bin/supervisord -n -c supervisord.conf
#CMD cron && crontab /etc/cron.d/fetch-cron && /bin/bash
CMD /bin/bash
