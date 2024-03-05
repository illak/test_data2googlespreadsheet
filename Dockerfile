FROM python:3.9.1

RUN apt-get install wget
RUN pip install --proxy http://10.250.5.7:8080 numpy pandas sqlalchemy psycopg2 python-dotenv mysql-connector PyMySQL google_spreadsheet google-auth-oauthlib

WORKDIR /script
COPY . .

ENV HTTP_PROXY=http://10.250.5.7:8080
ENV HTTPS_PROXY=https://10.250.5.7:8080

ENTRYPOINT [ "python", "main.py" ]