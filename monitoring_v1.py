import mysql.connector
#import random
#import numpy as np
from scipy import stats
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Query
from starlette.staticfiles import StaticFiles

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Allow CORS if you need cross-origin access from your LAN
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_CONFIG = {
    'host': '192.168.1.38',
    'user': 'readonly_user',
    'password': 'kts@tsd2025'
}

def fetch_operator_en_today(start_date = None, end_date = None):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SHOW DATABASES")
    all_databases = [db[0]for db in cursor.fetchall()]
    hide_table = ['smt','onanofflimited']
    databases = [db for db in all_databases if db not in hide_table]

    all_data = []
    active_databases = []
    models = []
    stations = []
    today_str = datetime.now().strftime('%Y-%m-%d')

    for db in databases:
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

                where_clause = f"DATE(`{date_column}`) = CURDATE()"
                params = ()
                if start_date and end_date:
                    where_clause = f"DATE(`{date_column}`) BETWEEN %s AND %s"
                    params = (start_date, end_date)
                else:
                    where_clause = f"DATE(`{date_column}`) = CURDATE()"
                    params = ()

                # Main grouped query
                query = f"""
                    SELECT 
                        operator_en, 
                        COUNT(DISTINCT serial_num) as Current_Output, 
                        MIN(`{date_column}`) as start_time,
                        MAX(`{date_column}`) as end_time,
                        TIMESTAMPDIFF(HOUR, MIN(`{date_column}`), NOW()) as duration_hours
                    FROM `{db}`.`{table}`
                    WHERE {where_clause}
                    AND `status` = 1
                    GROUP BY operator_en
                """
                cursor.execute(query, params)
                rows = cursor.fetchall()
                if rows and db not in active_databases:
                    active_databases.append(db)

                # For each operator_en, compute Duration_Per_Output (3 lowest)
                for row in rows:
                    operator_en, current_output, start_time, end_time, duration_hours = row

                    # Fetch all timestamps for this operator today
                    if start_date and end_date:
                        cursor.execute(f"""
                            SELECT serial_num, `{date_column}`
                            FROM `{db}`.`{table}`
                            WHERE DATE(`{date_column}`) BETWEEN %s AND %s
                            AND operator_en = %s
                            AND `status` = 1
                            ORDER BY `{date_column}`
                        """, (start_date, end_date, operator_en))
                    else:
                        cursor.execute(f"""
                            SELECT serial_num, `{date_column}`
                            FROM `{db}`.`{table}`
                            WHERE DATE(`{date_column}`) = CURDATE()
                            AND operator_en = %s
                            AND `status` = 1
                            ORDER BY `{date_column}`
                        """, (operator_en,))


                    records = cursor.fetchall()
                    serial_nums = [r[0] for r in records]
                    timestamps = [r[1] for r in records]

                    # Fetch process_time from production_plan.target_time
                    # Split the project name into model and station
                    project_name = table
                    if '_' in project_name:
                        model, station = project_name.split('_', 1)
                    else:
                        model, station = project_name, ''

                    # Fetch process_time from production_plan.target_time
                    try:
                        cursor.execute("""
                            SELECT process_time 
                            FROM production_plan.target_time
                            WHERE customer = %s AND model = %s AND station = %s
                            LIMIT 1
                        """, (db, model, station))
                        result = cursor.fetchone()
                        target_output = result[0] if result else None
                    except mysql.connector.Error:
                        target_output = None

                    # Calculate durations between consecutive records
                    durations = []
                    for i in range(1, len(timestamps)):
                        diff = (timestamps[i] - timestamps[i-1]).total_seconds()
                        if diff > 0:
                            durations.append(diff)

                    # Get 3 shortest durations and compute average
                    durations.sort()
                    #avg_3_shortest = round(sum(durations[:3]) / 3, 2) if len(durations) >= 3 else 0
                    avg_3_shortest = (end_time - start_time).total_seconds() / current_output
                    cycle_time_display = '-' if avg_3_shortest == 0 else avg_3_shortest
                    '''modes = np.round(durations, 2)
                    modes_results = stats.mode(modes, keepdims=False)
                    mode_value = float(modes_results.mode)'''
                    
                    # Set status
                    if target_output is not None:
                        status = "ON TARGET" if avg_3_shortest <= target_output else "BELOW TARGET"
                    else:
                        status = "NO TARGET"

                    # Split the project name into model and station
                    project_name = table
                    if '_' in project_name:
                        model, station = project_name.split('_', 1)
                    else:
                        model, station = project_name, ''

                    all_data.append({
                        'Customer': db.upper(),
                        'Model': model.upper(),
                        'Station': station.upper(),
                        'Operator': operator_en,
                        'Output': current_output,
                        'Target(s)': target_output,
                        'Cycle Time(s)': f"{cycle_time_display:.2f}"if cycle_time_display != '-' else cycle_time_display,
                        'Start Time': start_time.strftime('%H:%M:%S')if start_time else None,#str(start_time),
                        'End time': end_time.strftime('%H:%M:%S')if end_time else None,#str(end_time),
                        'Status': status,
                        'serial_num': serial_nums
                    })

                    models = sorted(set(entry['Model'] for entry in all_data))  # Clear modes for next iteration
                    stations = sorted(set(entry['Station'] for entry in all_data))  # Clear stations for next iteration

            except mysql.connector.Error:
                continue
    
    cursor.close()
    conn.close()
    return all_data, today_str, active_databases, models, stations

@app.get("/", response_class=HTMLResponse)
async def show_operator_en_today(request: Request, start_date: str = Query(None), end_date: str = Query(None), customer: str = Query(None), model: str = Query(None), station: str = Query(None)):
    all_data, date_str, active_databases, models, stations = fetch_operator_en_today(start_date, end_date)

    if customer:
        all_data = [d for d in all_data if d['Customer'] == customer]
    if model:
        all_data = [d for d in all_data if d['Model'] == model]
    if station:
        all_data = [d for d in all_data if d['Station'] == station]

    columns = ['Customer', 'Model', 'Station', 'Operator', 'Output', 'Cycle Time(s)', 'Target(s)', 'Start Time', 'End time', 'Status']
    rows = [tuple(d[col] for col in columns) for d in all_data]

    # Display range like "2025-08-25 → 2025-08-27"
    current_date_display = f"{start_date} → {end_date}" if start_date and end_date else datetime.now().strftime('%Y-%m-%d')

    return templates.TemplateResponse("monitoring_v1.html", {
        "request": request,
        "rows": rows,
        "columns": columns,
        "current_date": current_date_display,
        "start_date": start_date,
        "end_date": end_date,
        "selected_db": customer,
        "selected_model": model,
        "selected_station": station,
        "db": active_databases,  # optionally pass the db list
        "models": models,
        "stations": stations
    })


@app.get("/api/operator_today", response_class=JSONResponse)
async def api_operator_today():
    all_data, today_str = fetch_operator_en_today()
    return {
        "date": today_str,
        "count": len(all_data),
        "records": all_data
    }

@app.get("/operator/{operator_name}", response_class=HTMLResponse)
async def show_operator_activity(request: Request, operator_name: str):
    all_data, today_str = fetch_operator_en_today()
    filtered = [d for d in all_data if d['Operator'] == operator_name]

    columns = ['Customer', 'Model', 'Station', 'Operator', 'Output', 
               'Cycle Time(s)', 'Target(s)', 'Start Time', 'End time', 'Status']
    rows = [tuple(d[col] for col in columns) for d in filtered]

    # Include serials
    serials = []
    for d in filtered:
        serials.extend(d.get("serial_num", []))

    return templates.TemplateResponse("operator_activity.html", {
        "request": request,
        "rows": rows,
        "columns": columns,
        "current_date": today_str,
        "operator_name": operator_name,
        "serials": serials
    })

@app.get("/api/operator_outputs/{operator_name}", response_class=JSONResponse)
async def api_operator_outputs(operator_name: str):
    all_data, _ = fetch_operator_en_today()
    filtered = [d for d in all_data if d['Operator'] == operator_name]

    outputs = []
    for record in filtered:
        # Assume you already fetch `serial_num`, `start_time`, `end_time`, and `Cycle Time(s)` in your fetch_operator_en_today()
        serials = record.get("serial_num", [])
        cycle_time = record.get("Cycle Time(s)", None)

        for idx, serial in enumerate(serials, 1):
            outputs.append({
                "serial_num": serial,
                "cycle_time": cycle_time,
                "operator": operator_name,
                "order": idx
            })

    return {"operator": operator_name, "outputs": outputs}

@app.get("/api/operator_summary", response_class=JSONResponse)
async def api_operator_summary():
    all_data, _ = fetch_operator_en_today()
    summary = {}

    for record in all_data:
        operator = record["Operator"]
        output = record["Output"]
        summary[operator] = summary.get(operator, 0) + output

    return {"summary": summary}

@app.get("/api/get-models-stations", response_class=JSONResponse)
async def get_models_and_stations(customer: str):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Use only the selected customer
        query = f"SHOW TABLES FROM `{customer}`"
        cursor.execute(query)
        tables = [tbl[0] for tbl in cursor.fetchall()]

        models = set()
        stations = set()

        for table in tables:
            if '_' in table:
                model, station = table.split('_', 1)
            else:
                model, station = table, ''

            models.add(model.upper())
            stations.add(station.upper())

        return {
            "models": sorted(models),
            "stations": sorted(stations)
        }

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
