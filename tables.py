# -*- coding: utf-8 -*-
from sqlalchemy import *
from sqlalchemy import create_engine as ce
from sqlalchemy import inspect
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd

# SQLAlchemy connection
engine = ce('mysql://root:2002@localhost/test', echo=True)
Base = declarative_base()
metadata = MetaData(engine)


def tables_check():
    # Users table
    try:
        users_csv = pd.read_csv(r'csv/users.csv', header=0, parse_dates=['registration_date'], dayfirst=True)
        df = pd.DataFrame(users_csv)
        df.to_sql('users', con=engine, index=False, if_exists='fail', chunksize=500)
    except Exception:
        print("Users table exists")

    # Payments table
    try:
        payments_csv = pd.read_csv(r'csv/payments.csv', header=0, parse_dates=['payment_date'], dayfirst=True)
        df = pd.DataFrame(payments_csv)
        df.to_sql('payments', con=engine, index=False, if_exists='fail', chunksize=500)
    except Exception:
        print("Payments table exists")

    # Credits table
    try:
        credits_csv = pd.read_csv(r'csv/credits.csv', header=0, parse_dates=['issuance_date', 'return_date', 'actual_return_date'], dayfirst=True)
        df = pd.DataFrame(credits_csv)
        df.to_sql('credits', con=engine, index=False, if_exists='fail', chunksize=500)
    except Exception:
        print("Credits table exists")

    # Plans table
    try:
        plans_csv = pd.read_csv(r'csv/plans.csv', header=0, parse_dates=['period'], dayfirst=True)
        df = pd.DataFrame(plans_csv)
        df.to_sql('plans', con=engine, index=False, if_exists='fail', chunksize=12)
    except Exception:
        print("Plans table exists")

    # Dictionary table
    try:
        dictionary_csv = pd.read_csv(r'csv/dictionary.csv', header=0)
        df = pd.DataFrame(dictionary_csv)
        df.to_sql('dictionary', con=engine, index=False, if_exists='fail')
    except Exception:
        print("Dictionary table exists")
