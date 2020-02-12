FROM python:3.7
#FROM python:3.7-slim

RUN apt-get update

# place to keep our app and the data:
RUN mkdir -p /app /data /data/logs /_tmp

# copy over the secrets and the code
#COPY ["secrets.json", "kowalski/*_api.*", "kowalski/generate_secrets.py",\
#      "kowalski/middlewares.py", "kowalski/utils.py", "kowalski/api.py",\
#      "tests/test_api.py",
#      "/app/"]
COPY ["secrets.json", "kowalski/*_api.*", "kowalski/generate_secrets.py",\
      "kowalski/middlewares.py", "kowalski/utils.py",\
      "/app/"]

# change working directory to /app
WORKDIR /app

# install python libs and generate keys
RUN pip install -r /app/requirements_api.txt --no-cache-dir && python generate_secrets.py

COPY kowalski/api.py /app/

# run tests
#RUN python -m pytest -s test_api.py

# run container
#CMD python api.py
CMD /usr/local/bin/supervisord -n -c supervisord_api.conf
