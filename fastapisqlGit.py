import json
import pymysql
from fastapi import FastAPI, Query, HTTPException
import uvicorn, os
from pydantic import BaseModel
from typing import Optional

app = FastAPI()


class Person(BaseModel):
    account_number: Optional[int] = None
    account_type: str
    customer_number: int
    account_record_date: int
    status_indicator: str
    invoice_type: str
    legacy_account: str


# replace the db_host with public IP for local and the private one for the Cloud

con = pymysql.connect(host="{{db_host}}", user="{{db_root}}", password="{{db_password}}", db="db_name")
cursor = con.cursor()

with open('Customer.json', 'r') as f:
    customer = json.load(f)


# Gets customer from JSON
@app.get('/customer/{customer_account_number}')
def get_customer(customer_account_number: int):
    person = [c for c in customer if c['account_number'] == customer_account_number]
    return person[0] if len(person) > 0 else {}


# Searches for customer
@app.get("/search", status_code=200)
def search_person(account_number: Optional[int] = Query(None, title="account_number",
                                                        description="The account number to filter by"),
                  account_type: Optional[str] = Query(None, title="account_type",
                                                      description="The account type to filter by")):
    customer1 = [c for c in customer if c['account_number'] == account_number]
    if account_type is None:
        if account_number is None:
            return customer
        else:
            return customer1
    else:
        customer2 = [c for c in customer if account_type.lower() in c['account_type'].lower()]
        if account_number is None:
            return customer2
        else:
            combined = [c for c in customer1 if c in customer2]
            return combined


# Add one customer to the DB
@app.post('/addCustomer')
def add_customer(person: Person):
    new_customer = {
        "account_number": person.account_number,
        "account_type": person.account_type,
        "customer_number": person.customer_number,
        "account_record_date": person.account_record_date,
        "status_indicator": person.status_indicator,
        "invoice_type": person.invoice_type,
        "legacy_acoount": person.legacy_account
    }

    account_number = new_customer.get("account_number")
    account_type = new_customer.get("account_type")
    customer_number = new_customer.get("customer_number")
    account_record_date = new_customer.get("account_record_date")
    status_indicator = new_customer.get("status_indicator")
    invoice_type = new_customer.get("invoice_type")
    legacy_account = new_customer.get("legacy_account")
    insert_query = "INSERT INTO customer (account_number,account_type,customer_number,account_record_date,status_indicator,invoice_type,legacy_account) VALUES({0},'{1}',{2},{3},'{4}','{5}','{6}') ".format(
        account_number, account_type, customer_number, account_record_date, status_indicator, invoice_type,
        legacy_account)
    print(insert_query)
    cursor.execute(insert_query)
    con.commit()
    con.close()

    return


@app.put('/changeCustomer', status_code=204)
def change_customer(person: Person):
    new_customer = {
        "account_number": person.account_number,
        "account_type": person.account_type,
        "customer_number": person.customer_number,
        "account_record_date": person.account_record_date,
        "invoice_type": person.invoice_type,
        "legacy_account": person.legacy_account
    }

    customer_list = [c for c in customer if c['account_number'] == person.account_number]
    if len(customer_list) > 0:
        customer.remove(customer_list[0])
        customer.append(new_customer)
        with open('Customer.json', 'w') as f:
            json.dump(customer, f)
        return new_customer
    else:
        return HTTPException(status_code=404, detail=f"Customer with id {person.account_number} does not Exit!")


# Get all customres from the JSON file
@app.get('/customer')
def get_all_customers():
    person = [c for c in customer]
    return person


# Get all customers from DB
@app.get('/dbcustomer')
def get_all_db_customers():
    query = "SELECT * FROM customer"
    print(query)
    cursor.execute(query)
    person = cursor.fetchall()
    con.close()
    return person

# Get one Customer from the DB
@app.get('/dbcustomer/{customer_account_number}')
def get_one_db_customers(customer_account_number: int):
    query = "SELECT * FROM customer where customer_account_number={0}".format(customer_account_number)
    print(query)
    cursor.execute(query)
    person = cursor.fetchone()
    con.close()
    return person

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
def load_customer():
    person = [c for c in customer]
    #  Loading the list onto the DB
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
    con.close()


if __name__ == "__main__":
    uvicorn.run(app, port=int(os.environ.get("PORT", 8000)), host="0.0.0.0")
