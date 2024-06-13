from flask import Flask, request, render_template
import pymssql
import pandas as pd

app = Flask(__name__)

def get_db_connection():
    conn = pymssql.connect(host='hongshuo1.database.windows.net', user='hongshuo', password='Ligengxi456', database='cloud')
    return conn

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']
    if file:
        data = pd.read_csv(file)
        conn = get_db_connection()
        cursor = conn.cursor()
        for _, row in data.iterrows():
            cursor.execute(f"INSERT INTO ass2.earthquakes VALUES ('{row['time']}', {row['latitude']}, {row['longitude']}, {row['depth']}, {row['mag']}, '{row['magType']}', {row['nst']}, {row['gap']}, {row['dmin']}, {row['rms']}, '{row['net']}', '{row['id']}', '{row['updated']}', '{row['place']}', '{row['type']}', {row['horizontalError']}, {row['depthError']}, {row['magError']}, {row['magNst']}, '{row['status']}', '{row['locationSource']}', '{row['magSource']}')")
        conn.commit()
        conn.close()
        return 'Data uploaded successfully'
    return 'Failed to upload data'

@app.route('/query', methods=['GET'])
def query():
    mag = request.args.get('mag')
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    cursor.execute(f"SELECT * FROM ass2.earthquakes WHERE mag > {mag}")
    rows = cursor.fetchall()
    conn.close()
    return render_template('query.html', rows=rows)

if __name__ == '__main__':
    app.run(debug=True)
