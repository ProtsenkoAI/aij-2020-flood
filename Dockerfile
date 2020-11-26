FROM python:3.8


#RUN apt-get update && apt-get install -y --no-install-recommends 
#RUN pip install pipenv
#RUN pipenv install

COPY Pipfile ./
COPY Pipfile.lock ./

COPY . .

ENTRYPOINT ["python", "./forecast.py"]
