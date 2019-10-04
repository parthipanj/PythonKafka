# Python Base Image
FROM python:3

# Work Directory
WORKDIR /usr/src/app

COPY requirements.txt ./

RUN apt-get -y update && apt-get -y install libsnappy-dev

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./mykafka-python.py" ]
