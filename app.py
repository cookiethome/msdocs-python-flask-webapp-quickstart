from flask import Flask, request, render_template
import pymssql
import pandas as pd
from datetime import datetime
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///people.db'
app.config['SECRET_KEY'] = 'your_secret_key'
db = SQLAlchemy(app)

def get_db_connection():
    # 请将以下数据库连接信息修改为你的实际信息
    conn = pymssql.connect(
        host='hongshuo1.database.windows.net',
        user='hongshuo',
        password='Ligengxi456',
        database='cloud'
    )
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
            cursor.execute(f"""
                INSERT INTO ass2.earthquakes (time, latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net, id, updated, place, type, horizontalError, depthError, magError, magNst, status, locationSource, magSource) 
                VALUES ('{row['time']}', {row['latitude']}, {row['longitude']}, {row['depth']}, {row['mag']}, '{row['magType']}', {row['nst']}, {row['gap']}, {row['dmin']}, {row['rms']}', '{row['net']}', '{row['id']}', '{row['updated']}', '{row['place']}', '{row['type']}', {row['horizontalError']}, {row['depthError']}, {row['magError']}, {row['magNst']}', '{row['status']}', '{row['locationSource']}', '{row['magSource']}')
            """)
        conn.commit()
        conn.close()
        return 'Data uploaded successfully'
    return 'Failed to upload data'
 #dsafag
@app.route('/query', methods=['GET'])
def query():
    mag = request.args.get('mag')
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    cursor.execute(f"SELECT * FROM ass2.earthquakes WHERE mag > {mag}")
    rows = cursor.fetchall()
    conn.close()
    return render_template('query.html', rows=rows)

@app.route('/count_mag_5', methods=['GET'])
def count_mag_5():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as count FROM ass2.earthquakes WHERE mag > 5.0")
    result = cursor.fetchone()
    conn.close()
    return f"Earthquakes with magnitude > 5.0: {result[0]}"

@app.route('/range_query', methods=['GET'])
def range_query():
    mag_min = float(request.args.get('mag_min'))
    mag_max = float(request.args.get('mag_max'))
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    query = f"SELECT * FROM ass2.earthquakes WHERE mag BETWEEN {mag_min} AND {mag_max}"
    if start_date:
        query += f" AND time >= '{start_date}'"
    if end_date:
        query += f" AND time <= '{end_date}'"
    
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    return render_template('query.html', rows=rows)

@app.route('/near_location', methods=['GET'])
def near_location():
    lat = float(request.args.get('lat'))
    lon = float(request.args.get('lon'))
    distance_km = float(request.args.get('distance_km'))
    
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    
    query = f"""
        SELECT *, 
            (6371 * acos(cos(radians({lat})) * cos(radians(latitude)) * cos(radians(longitude) - radians({lon})) + sin(radians({lat})) * sin(radians(latitude)))) AS distance 
        FROM ass2.earthquakes 
        HAVING distance < {distance_km}
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    return render_template('query.html', rows=rows)

@app.route('/night_quakes', methods=['GET'])
def night_quakes():
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    query = """
        SELECT * FROM ass2.earthquakes 
        WHERE mag > 4.0 AND (DATEPART(hour, time) >= 18 OR DATEPART(hour, time) <= 6)
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    return render_template('query.html', rows=rows)

if __name__ == '__main__':
    app.run(debug=True)
