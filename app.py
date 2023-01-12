from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from flask import Flask, request
from flask_apscheduler import APScheduler
from flask_mail import Mail, Message
import os
import psycopg2
import psycopg2.extras
import pytz

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

DELETE_EVENT = (
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

DELETE_RECIPIENT = (
    "DELETE FROM RECIPIENTS WHERE ID = %s;"
)

load_dotenv()

app = Flask(__name__)
app.config['MYSQL_CURSORCLASS'] = 'DictCursor'
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USERNAME'] = os.getenv("SENDER_EMAIL")
app.config['MAIL_PASSWORD'] = os.getenv("SENDER_PASSWORD")
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USE_SSL'] = False

db_url = os.getenv("DATABASE_URL")
connection = psycopg2.connect(db_url)
sched = APScheduler()
mail = Mail(app)
tz = pytz.timezone('Asia/Singapore')


@app.get('/')
def home():
    return "Hello world!"


@app.post('/save_emails')
def create_event():
    data = request.get_json()
    event_id = data['event_id']
    email_subject = data['email_subject']
    email_content = data['email_content']
    try:
        timestamp = tz.localize(
            datetime.strptime(data['timestamp'], '%d-%m-%Y %H:%M'), is_dst=None
        )
    except KeyError:
        return {
            "data": data,
            "message": 'Use this format for timestamp: day-month-year hour:minute UTC+8 (Asia/Singapore)',
        }, 400
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_EVENTS_TABLE)
            cursor.execute(
                INSERT_EVENT, (event_id, email_subject, email_content, timestamp))
    data['timestamp'] = data['timestamp'] + ' UTC+8 (Asia/Singapore)'
    return {
        "data": data,
        "message": "Event created successfully.",
    }, 201


@app.get("/view_emails")
def get_all_events():
    with connection:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(GET_ALL_EVENTS)
            data = cursor.fetchall()
    return {
        "data": data,
        "message": "All events retrieved successfully.",
    }, 200


@app.delete("/delete_emails/<int:id>")
def delete_event(id):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                DELETE_EVENT, (id,))
    return {
        "data": id,
        "message": "Event deleted successfully.",
    }, 201


@app.post('/save_recipients')
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


@app.get('/view_recipients')
def get_all_recipients():
    with connection:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(GET_ALL_RECIPIENTS)
            data = cursor.fetchall()
    return {
        "data": data,
        "message": "All recipients retrieved successfully.",
    }, 200


@app.delete("/delete_recipients/<int:id>")
def delete_recipient(id):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                DELETE_RECIPIENT, (id,))
    return {
        "data": id,
        "message": "Recipient deleted successfully.",
    }, 201


def check_events():
    with app.app_context():
        with connection:
            with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(GET_ALL_EVENTS)
                event_list = cursor.fetchall()
        with connection:
            with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(GET_ALL_RECIPIENTS)
                recipient_list = cursor.fetchall()
        recipient_email_list = []
        for recipient in recipient_list:
            recipient_email_list.append(recipient['email'])
        current_datetime = datetime.now(pytz.UTC)
        for event in event_list:
            event_singapore_time = event['timestamp'].replace(
                tzinfo=pytz.UTC)
            if event_singapore_time <= current_datetime:
                msg = Message(
                    event['email_subject'],
                    sender=os.getenv("SENDER_EMAIL"),
                    recipients=recipient_email_list,
                    body=event['email_content']
                )
                mail.send(msg)
                delete_event(event['id'])
                print('Email with id {} successfully sent to'.format(
                    event['id']), recipient_email_list)


if __name__ == '__main__':

    sched.add_job(id='check_events', func=check_events,
                  trigger='interval', minutes=1)
    sched.start()
    app.run(debug=True, use_reloader=False)
