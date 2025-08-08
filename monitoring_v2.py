import mysql.connector
import random
import numpy as np
import pandas as pd
from io import StringIO
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
from scipy import stats
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
#from PIL import Image, ImageDraw
#from colorama import Fore, Style, init

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
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

def fetch_operator_en_today():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SHOW DATABASES")
    databases = [db[0] for db in cursor.fetchall()]

    all_data = []
    today_str = datetime.now().strftime('%Y-%m-%d')

    for db in databases:
        cursor.execute(f"USE `{db}`")
        cursor.execute("SHOW TABLES")
        tables = [tbl [0] for tbl in cursor.fetchall()]
        #print
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

                # Main grouped query
                query = f"""
                    SELECT 
                        operator_en, 
                        COUNT(DISTINCT serial_num) as Current_Output, 
                        MIN(`{date_column}`) as start_time,
                        MAX(`{date_column}`) as end_time,
                        TIMESTAMPDIFF(HOUR, MIN(`{date_column}`), NOW()) as duration_hours
                    FROM `{db}`.`{table}`
                    WHERE DATE(`{date_column}`) = CURDATE()
                    AND `status` = 1
                    GROUP BY operator_en
                """
                cursor.execute(query)
                rows = cursor.fetchall()

                # For each operator_en, compute Duration_Per_Output (3 lowest)
                for row in rows:
                    operator_en, current_output, start_time, end_time, duration_hours = row

                    # Fetch all timestamps for this operator today
                    cursor.execute(f"""
                        SELECT `{date_column}`
                        FROM `{db}`.`{table}`
                        WHERE DATE(`{date_column}`) = CURDATE() 
                        AND operator_en = %s
                        AND `status` = 1
                        ORDER BY `{date_column}`
                    """, (operator_en,))
                    timestamps = [r[0] for r in cursor.fetchall()]
                    #Random Numbers
                    Target_Output = random.randint(20, 150)
                    status = "ON TARGET" if Target_Output <= current_output else "BELOW TARGET"
                    #status = Image.new("RGB", (200,100), "WHITE")
                    #draw = ImageDraw.Draw(status)
                    #if Target_Output != current_output:
                    #    draw.ellipse((20,20,80,80), fill= 'red')
                    #else:
                    #    draw.ellipse((120,20,180,80), fill='green')
                    #init(autoreset=True)
                    #status = 0
                    #if Target_Output <= current_output:
                        #return status.Fore.GREEN
                    #else:
                        #status.Fore.RED
                    # Calculate durations between consecutive records
                    durations = []
                    for i in range(1, len(timestamps)):
                        diff = (timestamps[i] - timestamps[i-1]).total_seconds()
                        if diff > 0:
                            durations.append(diff)

                    # Get 3 shortest durations and compute average
                    durations.sort()
                    avg_3_shortest = round(sum(durations[:3]) / 3, 2) if len(durations) >= 3 else 0
                    modes = np.round(durations, 2)
                    modes_results = stats.mode(modes, keepdims=False)
                    mode_value = float(modes_results.mode)
                    #duration_per_unit = round(duration_hours / current_output, 2) if current_output else 0

                    all_data.append({
                        'Customer': db,
                        'Project': table,
                        'operator_en': operator_en,
                        'Target_Output': Target_Output,
                        'Current_Output': current_output,
                        'Process_Time(s)': avg_3_shortest,
                        'Start_Time': str(start_time),
                        'End_time': str(end_time),
                        'Status': status,
                        #'Status_modes': mode_value
                        #'Duration_Per_Unit': duration_per_unit
                    })

            except mysql.connector.Error:
                continue
    
    #status.show()
    cursor.close()
    conn.close()
    return all_data, today_str
@app.get("/download-csv")
def download_csv():
    all_data, today_str = fetch_operator_en_today()

    # Define columns in the same order as your HTML table
    columns = ['Customer', 'Project', 'operator_en', 'Target_Output', 'Current_Output', 'Process_Time(s)', 'Start_Time', 'End_time', 'Status']
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data, columns=columns)

    # Convert DataFrame to CSV in memory
    stream = StringIO()
    df.to_csv(stream, index=False)
    stream.seek(0)

    # Return CSV as downloadable response
    return StreamingResponse(stream, media_type="text/csv", headers={
        "Content-Disposition": f"attachment; filename=operator_data_{today_str}.csv"
    })

@app.get("/", response_class=HTMLResponse)
async def show_operator_en_today(request: Request):
    all_data, today_str = fetch_operator_en_today()
    columns = ['Customer', 'Project', 'operator_en','Target_Output', 'Current_Output', 'Process_Time(s)', 'Start_Time', 'End_time', 'Status']
    rows = [tuple(d[col] for col in columns) for d in all_data]
    return templates.TemplateResponse("monitoring_v2.html", {
        "request": request,
        "rows": rows,
        "columns": columns,
        "current_date": today_str
    })

@app.get("/api/operator_today", response_class=JSONResponse)
async def api_operator_today():
    all_data, today_str = fetch_operator_en_today()
    return {
        "date": today_str,
        "count": len(all_data),
        "records": all_data
    }

#if __name__ == "__main__":
#    app.run(debug=True)