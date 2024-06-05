from flask import Flask, request, redirect, url_for, render_template, flash
from flask_sqlalchemy import SQLAlchemy
from azure.storage.blob import BlobServiceClient
import pandas as pd
import os

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///people.db'
app.config['SECRET_KEY'] = 'your_secret_key'
db = SQLAlchemy(app)

connect_str = "<your_connection_string>"
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_name = "people-container"
container_client = blob_service_client.get_container_client(container_name)

# Ensure the container exists
try:
    container_client.create_container()
except Exception as e:
    pass

class Person(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)
    salary = db.Column(db.Integer, nullable=False)
    room = db.Column(db.Integer, nullable=False)
    telnum = db.Column(db.Integer, nullable=False)
    picture = db.Column(db.String(100), nullable=False)
    keywords = db.Column(db.String(200), nullable=False)

db.create_all()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)
    file = request.files['file']
    if file.filename == '':
        flash('No selected file')
        return redirect(request.url)
    if file and allowed_file(file.filename):
        blob_client = container_client.get_blob_client(file.filename)
        blob_client.upload_blob(file)
        if file.filename.endswith('.csv'):
            process_csv(file)
        flash('File successfully uploaded')
        return redirect(url_for('index'))
    flash('File type not allowed')
    return redirect(request.url)

@app.route('/query', methods=['GET', 'POST'])
def query():
    if request.method == 'POST':
        name = request.form.get('name')
        salary = request.form.get('salary')
        if name:
            person = Person.query.filter_by(name=name).first()
            if person:
                blobs = [person.picture]
            else:
                blobs = []
        elif salary:
            people = Person.query.filter(Person.salary < int(salary)).all()
            blobs = [person.picture for person in people]
        return render_template('query.html', blobs=blobs)
    return render_template('query.html')

@app.route('/modify', methods=['POST'])
def modify():
    action = request.form.get('action')
    name = request.form.get('name')
    if action == 'add':
        # Add new record logic
        pass
    elif action == 'remove':
        # Remove record logic
        pass
    elif action == 'update':
        # Update record logic
        pass
    return redirect(url_for('index'))

def process_csv(file):
    df = pd.read_csv(file)
    for index, row in df.iterrows():
        person = Person(
            name=row['Name'],
            salary=row['Salary'],
            room=row['Room'],
            telnum=row['Telnum'],
            picture=row['Picture'],
            keywords=row['Keywords']
        )
        db.session.add(person)
    db.session.commit()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'csv', 'jpg', 'jpeg', 'png'}

if __name__ == '__main__':
    app.run(debug=True)
