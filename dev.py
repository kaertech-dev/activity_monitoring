from flask import Flask, render_template
import mysql.connector
from datetime import datetime

app = Flask(__name__)

DB_CONFIG = {
    'host': '192.168.2.5',
    'user': 'readonly_user',
    'password': 'kts@tsd2025'
}

@app.route('/')
def show_today_records():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("SHOW DATABASES")
        databases = [db[0] for db in cursor.fetchall()]
        
        all_data = {}
        today_str = datetime.now().strftime('%Y-%m-%d')

        for db in databases:
            cursor.execute(f"USE `{db}`")
            cursor.execute("SHOW TABLES")
            tables = [tbl[0] for tbl in cursor.fetchall()]
            
            db_data = {}
            for table in tables:
                try:
                    # Get column info
                    cursor.execute(f"DESCRIBE `{table}`")
                    columns_info = cursor.fetchall()
                    date_column = None

                    for col in columns_info:
                        col_name = col[0].lower()
                        col_type = col[1].lower()
                        if ('date' in col_name or 'time' in col_name) and \
                           ('date' in col_type or 'timestamp' in col_type):
                            date_column = col[0]
                            break

                    if date_column:
                        query = f"SELECT * FROM `{table}` WHERE DATE(`{date_column}`) = CURDATE()"
                        cursor.execute(query)
                        rows = cursor.fetchall()

                        if rows:
                            columns = [desc[0] for desc in cursor.description]
                            db_data[table] = {
                                'columns': columns,
                                'rows': rows,
                                'date_column': date_column
                            }

                except mysql.connector.Error as table_err:
                    db_data[table] = {
                        'columns': [],
                        'rows': [],
                        'error': str(table_err)
                    }

            if db_data:
                all_data[db] = db_data

        cursor.close()
        conn.close()
        return render_template("dev.html", all_data=all_data, current_date=today_str)

    except Exception as e:
        return f"Error: {e}"

if __name__ == "__main__":
    app.run(debug=True)
