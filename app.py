from flask import Flask, render_template
import mysql.connector
from datetime import datetime

app = Flask(__name__)

# MySQL read-only credentials
DB_CONFIG = {
    'host': '192.168.2.5',
    'user': 'readonly_user',
    'password': 'kts@tsd2025'
}

@app.route('/')
def show_all_data():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("SHOW DATABASES")
        databases = [db[0] for db in cursor.fetchall()]
        
        all_data = {}

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
                        if 'date' in col[0].lower() or 'time' in col[0].lower():
                            if 'date' in col[1].lower() or 'timestamp' in col[1].lower():
                                date_column = col[0]
                                break

                    if date_column:
                        query = (
                            f"SELECT * FROM `{table}` "
                            f"WHERE DATE(`{date_column}`) = CURDATE()"
                            f" ORDER BY `{date_column}` DESC"
                        )
                        
                        #f"SELECT * FROM `{table}` WHERE DATE(`{date_column}`) = CURDATE()"
                    else:
                        query = f"SELECT * FROM `{table}` LIMIT 10"  # fallback

                    cursor.execute(query)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    db_data[table] = {
                        'columns': columns,
                        'rows': rows,
                        'date_filtered': bool(date_column)
                    }

                except mysql.connector.Error as table_err:
                    db_data[table] = {
                        'columns': [],
                        'rows': [],
                        'error': str(table_err)
                    }

            all_data[db] = db_data

        cursor.close()
        conn.close()
        return render_template("databases.html", all_data=all_data)

    except Exception as e:
        return f"Error: {e}"
    
if __name__ == "__main__":
    app.run(debug=True)
