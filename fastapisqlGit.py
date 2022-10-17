import json
import pymysql
from fastapi import FastAPI, Query, HTTPException
import uvicorn, os
from pydantic import BaseModel
from typing import Optional

from DBConnection import DBConnection

app = FastAPI()


class Person(BaseModel):
    account_number: Optional[int] = None
    account_type: str
    customer_number: int
    account_record_date: int
    status_indicator: str
    invoice_type: str
    legacy_account: str


# DB connection

con = DBConnection.get_db_connection()
cursor = con.cursor()

with open('Customer.json', 'r') as f:
    customer = json.load(f)


# Gets customer from JSON
@app.get('/customer/{customer_account_number}')
def get_customer(customer_account_number: int):
    person = [c for c in customer if c['account_number'] == customer_account_number]
    return person[0] if len(person) > 0 else {}


# Searches for customer from DB
# Get one Customer from the DB
@app.get('/search/{customer_account_number}')
def get_one_db_customer(customer_account_number: int):
    query = "SELECT * FROM customer where account_number={0}".format(customer_account_number)
    print(query)
    cursor.execute(query)
    row_headers = [x[0] for x in cursor.description]
    result = cursor.fetchone()
    person_json = []
    person_json.append(dict(zip(row_headers, result)))
    return person_json


# Get all customers from DB
@app.get('/dbcustomer')
def get_all_customers_from_db():
    query = "SELECT * FROM customer"
    print(query)
    cursor.execute(query)
    row_headers = [x[0] for x in cursor.description]
    results = cursor.fetchall()
    person_json = []
    for result in results:
        person_json.append(dict(zip(row_headers, result)))
    return person_json


# Get all customers from the JSON file
@app.get('/customer')
def get_all_customers():
    person = [c for c in customer]
    return person


#
# @app.get("/search", status_code=200)
# def search_person(account_number: Optional[int] = Query(None, title="account_number",
#                                                         description="The account number to filter by"),
#                   account_type: Optional[str] = Query(None, title="account_type",
#                                                       description="The account type to filter by")):
#     customer1 = [c for c in customer if c['account_number'] == account_number]
#     if account_type is None:
#         if account_number is None:
#             return customer
#         else:
#             return customer1
#     else:
#         customer2 = [c for c in customer if account_type.lower() in c['account_type'].lower()]
#         if account_number is None:
#             return customer2
#         else:
#             combined = [c for c in customer1 if c in customer2]
#             return combined


# Add one customer to the DB

# Add one customer information to the Database
@app.post('/addCustomer')
def add_one_customer(person: Person):

    insert_query = "INSERT INTO customer (account_number,account_type,customer_number,account_record_date,status_indicator,invoice_type,legacy_account) VALUES({0},'{1}',{2},{3},'{4}','{5}','{6}') ".format(
        person.account_number, person.account_type, person.customer_number, person.account_record_date, person.status_indicator, person.invoice_type,person.legacy_account)
    print(insert_query)
    cursor.execute(insert_query)
    con.commit()

    return


@app.put('/changeCustomer', status_code=204)
def change_customer(person: Person):

    update_query="update customer set account_number={0}, account_type='{1}', customer_number={2},account_record_date={3},status_indicator='{4}',invoice_type='{5}',legacy_account='{6}' Where account_number={0};".format(
        person.account_number, person.account_type, person.customer_number, person.account_record_date, person.status_indicator, person.invoice_type, person.legacy_account)
    print(update_query)
    cursor.execute(update_query)
    con.commit()


# Delete for customer from DB
@app.delete('/delete/{customer_account_number}')
def delete_a_customer_from_db(customer_account_number: int):
    query = "DELETE FROM customer WHERE account_number={0}".format(customer_account_number)
    print(query)
    cursor.execute(query)
    con.commit()


# Delete JSON file entries
@app.delete('/deleteCustomer/{customer_account_number}', status_code=204)
def delete_person(customer_account_number: int):
    person = [c for c in customer if c['account_number'] == customer_account_number]
    if len(person) > 0:
        customer.remove(person[0])
        with open('Customer.json', 'w') as f:
            json.dump(customer, f)
    else:
        raise HTTPException(status_code=404,
                            detail=f"There is no customer with that {customer_account_number}customer account number")


# Loads all the customer list from the JSON onto the SQL
@app.post('/loadCustomer')
def load_all_customer_to_db():
    person = [c for c in customer]
    #  Loading to the DB
    for customersql in person:
        account_number = customersql.get("account_number")
        account_type = customersql.get("account_type")
        customer_number = customersql.get("customer_number")
        account_record_date = customersql.get("account_record_date")
        status_indicator = customersql.get("status_indicator")
        invoice_type = customersql.get("invoice_type")
        legacy_account = customersql.get("legacy_account")
        insert_query = "INSERT INTO customer (account_number,account_type,customer_number,account_record_date,status_indicator,invoice_type,legacy_account) VALUES({0},'{1}',{2},{3},'{4}','{5}','{6}') ".format(
            account_number, account_type, customer_number, account_record_date, status_indicator, invoice_type,
            legacy_account)
        print(insert_query)
        cursor.execute(insert_query)
    con.commit()


if __name__ == "__main__":
    uvicorn.run(app, port=int(os.environ.get("PORT", 8000)), host="0.0.0.0")
