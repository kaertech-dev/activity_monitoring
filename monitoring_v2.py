import mysql.connector
import random
import numpy as np
import pandas as pd
from io import StringIO
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Query
from scipy import stats
from datetime import datetime

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")
# Allow CORS if you need cross-origin access from your LAN
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_CONFIG = {
    'host': '192.168.2.5',
    'user': 'readonly_user',
    'password': 'kts@tsd2025'
}

def fetch_operator_data(start_date: str, end_date: str, db_name: str = None):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SHOW DATABASES")
    databases = [db[0] for db in cursor.fetchall()]

    if db_name and db_name not in databases:
        raise ValueError(f"Database {db_name} not found")

    all_data = []

    # Use the selected database, or all if no specific database is chosen
    target_databases = [db_name] if db_name else databases

    for db in target_databases:
        cursor.execute(f"USE `{db}`")
        cursor.execute("SHOW TABLES")
        tables = [tbl[0] for tbl in cursor.fetchall()]

        for table in tables:
            try:
                cursor.execute(f"DESCRIBE `{table}`")
                columns_info = cursor.fetchall()
                column_names = [col[0] for col in columns_info]

                if "operator_en" not in column_names or "serial_num" not in column_names or "status" not in column_names:
                    continue

                date_column = None
                for col in columns_info:
                    col_name = col[0].lower()
                    col_type = col[1].lower()
                    if ('date' in col_name or 'time' in col_name) and ('date' in col_type or 'timestamp' in col_type):
                        date_column = col[0]
                        break

                if not date_column:
                    continue

                query = f"""
                    SELECT 
                        operator_en, 
                        COUNT(DISTINCT serial_num) as Output, 
                        MIN(`{date_column}`) as start_time,
                        MAX(`{date_column}`) as end_time,
                        TIMESTAMPDIFF(HOUR, MIN(`{date_column}`), NOW()) as duration_hours
                    FROM `{db}`.`{table}`
                    WHERE DATE(`{date_column}`) BETWEEN %s AND %s
                    AND `status` = 1
                    GROUP BY operator_en
                """
                cursor.execute(query, (start_date, end_date))
                rows = cursor.fetchall()

                for row in rows:
                    operator_en, output, start_time, end_time, duration_hours = row

                    cursor.execute(f"""
                        SELECT `{date_column}`
                        FROM `{db}`.`{table}`
                        WHERE DATE(`{date_column}`) BETWEEN %s AND %s
                        AND operator_en = %s
                        AND `status` = 1
                        ORDER BY `{date_column}`
                    """, (start_date, end_date, operator_en))
                    timestamps = [r[0] for r in cursor.fetchall()]

                    Target_Output = random.randint(20, 150)
                    status = "ON TARGET" if Target_Output <= output else "BELOW TARGET"

                    durations = []
                    for i in range(1, len(timestamps)):
                        diff = (timestamps[i] - timestamps[i-1]).total_seconds()
                        if diff > 0:
                            durations.append(diff)

                    durations.sort()
                    avg_3_shortest = round(sum(durations[:3]) / 3, 2) if len(durations) >= 3 else 0
                    # Mode calculation
                    if durations:
                        modes = np.round(durations, 2)
                        modes_results = stats.mode(modes, keepdims=False)
                        mode_value = float(modes_results.mode) if not np.isnan(modes_results.mode) else 0
                    else:
                        mode_value = 0

                    all_data.append({
                        'Customer': db,
                        'Project': table,
                        'operator_en': operator_en,
                        'Target(s)': Target_Output,
                        'Output': output,
                        'Process_Time(s)': avg_3_shortest,
                        'Start_Time': str(start_time),
                        'End_time': str(end_time),
                        'Status': status
                    })

            except mysql.connector.Error:
                continue

    cursor.close()
    conn.close()
    return all_data, databases

#Backend
@app.get("/", response_class=HTMLResponse)
async def show_operator_en_today(
    request: Request,
    start_date: str = Query(default=datetime.now().strftime('%Y-%m-%d')),
    end_date: str = Query(default=datetime.now().strftime('%Y-%m-%d')),
    sort_by: str = Query(default="none"),
    db_name: str = Query(default=None)  # Accept the database name as a query parameter
):
    all_data, databases = fetch_operator_data(start_date, end_date, db_name)

    if sort_by == "az":
        all_data.sort(key=lambda x: x["operator_en"])
    elif sort_by == "za":
        all_data.sort(key=lambda x: x["operator_en"], reverse=True)
    elif sort_by == "time":
        all_data.sort(key=lambda x: x["Start_Time"])

    columns = ['Customer', 'Project', 'operator_en', 'Output', 'Process_Time(s)', 'Target(s)', 'Start_Time', 'End_time', 'Status']
    rows = [tuple(d[col] for col in columns) for d in all_data]

    return templates.TemplateResponse("monitoring_v2.html", {
        "request": request,
        "rows": rows,
        "columns": columns,
        "current_date": f"{start_date} → {end_date}",
        "sort_by": sort_by,  # pass to template so selected option stays
        "databases": databases,  # pass database list to the template
        "selected_db": db_name  # highlight selected database
    })



@app.get("/download-csv")
def download_csv(start_date: str = Query(...), end_date: str = Query(...)):
    all_data = fetch_operator_data(start_date, end_date)
    columns = ['Customer', 'Project', 'operator_en', 'Output', 'Process_Time(s)', 'Target(s)', 'Start_Time', 'End_time', 'Status']
    df = pd.DataFrame(all_data, columns=columns)
    stream = StringIO()
    df.to_csv(stream, index=False)
    stream.seek(0)
    return StreamingResponse(stream, media_type="text/csv", headers={
        "Content-Disposition": f"attachment; filename=operator_data_{start_date}_to_{end_date}.csv"
    })

@app.get("/api/operator_today", response_class=JSONResponse)
async def api_operator_today():
    all_data, today_str = fetch_operator_data()
    return {
        "date": today_str,
        "count": len(all_data),
        "records": all_data
    }


#if __name__ == "__main__":
#    app.run(host = "0.0.0.0", port=5000)