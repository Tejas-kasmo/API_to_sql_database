from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import pyodbc
import configparser as cp

app = FastAPI()

def connect():
    config = cp.ConfigParser()
    config.read(r'C:\Users\mysur\OneDrive\Desktop\python_tutorial\venv1\config.config')

    DRIVER = config['ssms']['DRIVER']
    SERVER = config['ssms']['SERVER']
    DATABASE = config['ssms']['DATABASE']
    UID = config['ssms']['UID']
    PWD = config['ssms']['PWD']

    conn = pyodbc.connect(
        f'DRIVER={DRIVER};'
        f'SERVER={SERVER};'
        f'DATABASE={DATABASE};'
        f'UID={UID};'
        f'PWD={PWD}'
    )

    return conn

class Customer(BaseModel):
    name: str
    email: str
    phone: str
    address: str
    registration_date: str
    loyalty_status: str

class CustomerOut(Customer):
    customer_id: int

@app.get("/customers/", response_model=List[CustomerOut])
def get_all_customers():
    conn = connect()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT customer_id, name, email, phone, address, registration_date, loyalty_status
        FROM dbo.us_customer_data
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    result = []
    for row in rows:
        result.append({
            "customer_id": row[0],
            "name": row[1],
            "email": row[2],
            "phone": row[3],
            "address": row[4],
            "registration_date": row[5],
            "loyalty_status": row[6]
        })

    return result

@app.get("/customers/{customer_id}", response_model=CustomerOut)
def get_customer(customer_id: int):
    conn = connect() 
    cursor = conn.cursor()
    cursor.execute("""
        SELECT customer_id, name, email, phone, address, registration_date, loyalty_status
        FROM dbo.us_customer_data
        WHERE customer_id = ?
    """, customer_id)
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if row:
        return {
            "customer_id": row[0],
            "name": row[1],
            "email": row[2],
            "phone": row[3],
            "address": row[4],
            "registration_date": row[5],
            "loyalty_status": row[6]
        }
    else:
        raise HTTPException(status_code=404, detail="Customer not found")

@app.post("/customers/", response_model=CustomerOut)
def create_customer(customer: Customer):
    conn = connect()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO dbo.us_customer_data (name, email, phone, address, registration_date, loyalty_status)
        OUTPUT INSERTED.customer_id
        VALUES (?, ?, ?, ?, ?, ?)
    """, customer.name, customer.email, customer.phone, customer.address, customer.registration_date, customer.loyalty_status)
    new_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    return {"customer_id": new_id, **customer.dict()}

@app.put("/customers/{customer_id}", response_model=CustomerOut)
def update_customer(customer_id: int, customer: Customer):
    conn = connect()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE dbo.us_customer_data
        SET name = ?, email = ?, phone = ?, address = ?, registration_date = ?, loyalty_status = ?
        WHERE customer_id = ?
    """, customer.name, customer.email, customer.phone, customer.address, customer.registration_date, customer.loyalty_status, customer_id)
    conn.commit()
    cursor.close()
    conn.close()
    return {"customer_id": customer_id, **customer.dict()}

@app.delete("/customers/{customer_id}")
def delete_customer(customer_id: int):
    conn = connect()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM dbo.us_customer_data WHERE customer_id = ?", customer_id)
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": f"Customer {customer_id} deleted"}
