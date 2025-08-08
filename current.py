from flask import Flask, render_template
import mysql.connector

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

        # Get all databases
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
                    cursor.execute(f"SELECT * FROM `{table}` LIMIT 10")
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    db_data[table] = {
                        'columns': columns,
                        'rows': rows
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
        return render_template("current.html", all_data=all_data)

    except Exception as e:
        return f"Error: {e}"

if __name__ == "__main__":
    app.run(debug=True)
