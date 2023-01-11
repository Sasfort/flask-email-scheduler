from datetime import datetime, timezone
from dotenv import load_dotenv
from flask import Flask, request
import os
import psycopg2
import psycopg2.extras

CREATE_EVENTS_TABLE = (
    """CREATE TABLE IF NOT EXISTS EVENTS (ID SERIAL PRIMARY KEY, EVENT_ID INTEGER, 
    EMAIL_SUBJECT TEXT, EMAIL_CONTENT TEXT, TIMESTAMP TIMESTAMP);"""
)

INSERT_EVENT = (
    """INSERT INTO EVENTS (EVENT_ID, EMAIL_SUBJECT, EMAIL_CONTENT, TIMESTAMP) 
    VALUES (%s, %s, %s, %s);"""
)

GET_ALL_EVENTS = (
    "SELECT * FROM EVENTS;"
)

DELETE_SENT_EVENT = (
    "DELETE FROM EVENTS WHERE ID = %s;"
)

CREATE_RECIPIENTS_TABLE = (
    "CREATE TABLE IF NOT EXISTS RECIPIENTS (ID SERIAL PRIMARY KEY, EMAIL TEXT);"
)

INSERT_RECIPIENT = (
    "INSERT INTO RECIPIENTS (EMAIL) VALUES (%s);"
)

GET_ALL_RECIPIENTS = (
    "SELECT * FROM RECIPIENTS;"
)

load_dotenv()
app = Flask(__name__)
app.config['MYSQL_CURSORCLASS'] = 'DictCursor'
db_url = os.getenv("DATABASE_URL")
connection = psycopg2.connect(db_url)


@app.get('/')
def home():
    return "Hello world!"


@app.post('/api/event')
def create_event():
    data = request.get_json()
    event_id = data['event_id']
    email_subject = data['email_subject']
    email_content = data['email_content']
    try:
        timestamp = datetime.strptime(data['timestamp'], '%d-%m-%Y %H:%M:%S')
    except KeyError:
        return {
            "data": data,
            "message": 'Use this format for timestamp: day-month-year hour:minute:second',
        }, 400
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_EVENTS_TABLE)
            cursor.execute(
                INSERT_EVENT, (event_id, email_subject, email_content, timestamp))
    return {
        "data": data,
        "message": "Event created successfully.",
    }, 201


@app.get("/api/event")
def get_all_events():
    with connection:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(GET_ALL_EVENTS)
            data = cursor.fetchall()
    return {
        "data": data,
        "message": "All events retrieved successfully.",
    }, 200


@app.post('/api/recipient')
def add_recipient():
    data = request.get_json()
    email = data['email']
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_RECIPIENTS_TABLE)
            cursor.execute(
                INSERT_RECIPIENT, (email,))
    return {
        "data": data,
        "message": "Recipient added successfully.",
    }, 201


@app.get('/api/recipient')
def get_all_recipients():
    with connection:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(GET_ALL_RECIPIENTS)
            data = cursor.fetchall()
    return {
        "data": data,
        "message": "All recipients retrieved successfully.",
    }, 200
