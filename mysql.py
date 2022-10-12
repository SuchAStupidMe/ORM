# -*- coding: utf-8 -*-

from sqlalchemy import create_engine as ce
from sqlalchemy import inspect
from datetime import date, datetime
import pandas as pd

file_db = ce('mysql://root:2002@localhost/test')


# Tables check
def tables_check(file_db, table):
    if not inspect(file_db).has_table(table):
        csv = pd.read_csv(f"csv/{table}.csv")
        csv.to_sql(f'{table}', file_db)
        print(f"Table {table} created")
    else:
        print(f"Table {table} exists")


def initial_check(tables: list):
    for table in tables:
        tables_check(file_db, table)


# User credit info
class Credit:
    def __init__(self, credit_id):
        self.credit_id = credit_id

        self.issuance_date = pd.read_sql_query(f"SELECT issuance_date FROM credits WHERE id = {self.credit_id}",
                                               file_db)
        self.issuance_date = self.issuance_date['issuance_date'].iloc[0]

        self.actual_return_date = pd.read_sql_query(
            f'SELECT actual_return_date FROM credits WHERE id = {self.credit_id}', file_db)
        self.actual_return_date = self.actual_return_date['actual_return_date'].iloc[0]

        if self.actual_return_date is not None:
            self.type = True

            self.body = pd.read_sql_query(f"SELECT body from credits WHERE id = {self.credit_id}", file_db)
            self.body = float(self.body['body'].iloc[0])

            self.percent = pd.read_sql_query(f"SELECT percent FROM credits WHERE id = {self.credit_id}", file_db)
            self.percent = float(self.percent['percent'].iloc[0])

            self.sum = pd.read_sql_query(f"SELECT sum FROM payments WHERE credit_id = {self.credit_id}", file_db)
            self.sum = sum(self.sum['sum'].tolist())

        else:
            self.type = False

            self.return_date = pd.read_sql_query(f"SELECT return_date FROM credits WHERE id = {self.credit_id}",
                                                 file_db)
            self.return_date = self.return_date['return_date'].iloc[0]

            self.overdue = (datetime.strptime(date.today().strftime("%d.%m.%Y"), "%d.%m.%Y") - datetime.strptime(
                self.return_date, "%d.%m.%Y")).days + 1

            self.body = pd.read_sql_query(f"SELECT body from credits WHERE id = {self.credit_id}", file_db)
            self.body = float(self.body['body'].iloc[0])

            self.percent = pd.read_sql_query(f"SELECT percent FROM credits WHERE id = {self.credit_id}", file_db)
            self.percent = float(self.percent['percent'].iloc[0])

            self.pay_type = pd.read_sql_query(f"SELECT type_id FROM payments WHERE credit_id = {self.credit_id}",
                                              file_db)
            self.pay_type = self.pay_type['type_id'].tolist()

            self.body_sum = pd.read_sql_query(
                f"SELECT sum FROM payments WHERE credit_id = {self.credit_id} AND type_id = 1", file_db)
            self.body_sum = float(sum(self.body_sum['sum'].tolist()))

            self.percent_sum = pd.read_sql_query(
                f"SELECT sum FROM payments WHERE credit_id = {self.credit_id} AND type_id = 2", file_db)
            self.percent_sum = float(sum(self.percent_sum['sum'].tolist()))


# Plan performance
class Performance:
    def __init__(self, date):
        self.date = pd.to_datetime(date, dayfirst=True)
        self.day, self.month, self.year = self.date.strftime('%d'), self.date.strftime('%m'), self.date.strftime('%Y')
        self.plan_3 = pd.read_sql_query(f"SELECT period, sum FROM plans WHERE period LIKE '__.{self.month}.{self.year}' AND category_id = 3", file_db)  # Видача
        self.plan_4 = pd.read_sql_query(f"SELECT period, sum FROM plans WHERE period LIKE '__.{self.month}.{self.year}' AND category_id = 4", file_db)  # Збір

        # Issuance
        self.issuance_sum = int(self.plan_3['sum'].iloc[0])
        self.issuance_performance = float(pd.read_sql_query(f"SELECT body FROM credits WHERE issuance_date LIKE '__.{self.month}.{self.year}' AND issuance_date <= '{self.day}.{self.month}.{self.year}'", file_db).sum().iloc[0])
        self.issuance_percentage = float(self.issuance_performance/self.issuance_sum * 100)

        # Issuance dict
        self.issuance = {
                        'Sum': self.issuance_sum,
                        'Performance': self.issuance_performance,
                        'Percentage': self.issuance_percentage
                        }

        # Gather
        self.gather_sum = int(self.plan_4['sum'].iloc[0])
        self.gather_performance = float(pd.read_sql_query(f"SELECT sum FROM payments WHERE payment_date LIKE '__.{self.month}.{self.year}' AND payment_date <= '{self.day}.{self.month}.{self.year}'", file_db).sum().iloc[0])
        self.gather_percentage = float(self.gather_performance/self.gather_sum * 100)

        # Gather dict
        self.gather ={
                     'Sum': self.gather_sum,
                     'Payments': self.gather_performance,
                     'Percentage': self.gather_percentage
                     }


def user_credits_search(file_db, user_id: int) -> dict:
    credits = pd.read_sql_query(f'SELECT id from credits WHERE user_id = {user_id}', file_db)
    credits = credits['id'].tolist()
    body = {"Credits": credits}
    for credit in credits:
        body[f"Credit {credit}"] = credit_info(credit)
    return body


def credit_info(credit_id) -> dict:
    credit = Credit(credit_id)

    if credit.type:
        return {
            "Credit id": credit.credit_id,
            "Issuance date": credit.issuance_date,
            "Type": credit.type,
            "Actual return date": credit.actual_return_date,
            "Body": credit.body,
            "Percent": credit.percent,
            "Sum": credit.sum
        }

    if not credit.type:
        return {
            "Credit id": credit.credit_id,
            "Issuance date": credit.issuance_date,
            "Type": credit.type,
            "Return date": credit.return_date,
            "Overdue": credit.overdue,
            "Body": credit.body,
            "Percent": credit.percent,
            "Body sum": credit.body_sum,
            "Percent sum": credit.percent_sum
        }


# Plans insert
def plan_insert(file_db) -> dict:
    try:
        new_plans = pd.read_excel('plan.xlsx')
        old_plans = pd.read_sql_query("SELECT period, category_id FROM plans", file_db)
        new_plans_category = []

        # Actuality check
        # Category names to id's
        for i in new_plans['category'].values:
            if i.lower() == 'видача':
                new_plans_category.append(3)
            elif i.lower() == 'збір':
                new_plans_category.append(4)

        new_plans['category_id'] = new_plans_category

        # Str period to datetime
        old_plans['period'] = pd.to_datetime(old_plans['period'], dayfirst=True)

        # Duplicates check
        old_plans_len = len(old_plans.index)
        new_plans_len = len(new_plans.index)
        dupe_len = len(pd.concat([old_plans, new_plans]).drop_duplicates(['period', 'category_id']).index)
        if old_plans_len + new_plans_len > dupe_len:
            return {"Error": "Plan Duplicates"}

        # Is month start
        for i in new_plans['period'].tolist():
            if not i.is_month_start:
                return {'Error': f'{i} is not month start'}

        # NaN sum check
        if new_plans['sum'].isnull().values.any():
            return {'Error': 'NaN values in sum'}

        # Insert into SQL Table
        for index, row in new_plans.iterrows():
            file_db.execute(
                f"INSERT INTO plans (period, sum, category_id) VALUES ('{row['period']}', {row['sum']}, {row['category_id']})")

        return {"Message": 'Success'}

    except Exception as e:
        return {'Error': e}


# Plans performance
def plan_performance(date) -> dict:
    plan = Performance(date)
    body = {
            'Month': plan.month,
            'Issuance': plan.issuance,
            'Gather': plan.gather
            }
    return body
