from peewee import (
    Model, MySQLDatabase, BigIntegerField, DateTimeField, CharField, DecimalField, IntegerField, AutoField, BooleanField
)
from dotenv import load_dotenv
import os
import pandas as pd

# Carrega variáveis do .env
load_dotenv()
host = os.getenv('HOST_2')
user = os.getenv('USER_2')
password = os.getenv('PASSWORD_2')
port = int(os.getenv('PORT_2'))
database = os.getenv('DATABASE_2')
db = MySQLDatabase(database=database, user=user, password=password,
                   host=host, port=port)