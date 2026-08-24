"""Business logic and data processing services - Refactored with Employee Names,
Active Project Filtering, Station Common Names, and _main-based Station Ordering"""
from typing import Optional, List, Dict, Tuple, Set
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from datetime import timedelta
import time

from models import ProductionRecord
from database import get_db_connection, lock
from data_processor import ProductionDataProcessor
from utils import get_production_start_time, get_production_date_range
from config import db_config

logger = logging.getLogger(__name__)

UTC_OFFSET_HOURS = 0

# How long (seconds) to trust the cached "active projects" / "station cnames" /
# "_main station order" lookups before re-querying. These change rarely, so a
# short TTL cache avoids hammering the DB without going permanently stale.
_METADATA_CACHE_TTL = 120

_active_projects_cache: Dict[str, Tuple[Set[str], float]] = {}
_station_cname_cache: Dict[str, Tuple[Dict[Tuple[str, str, str], str], float]] = {}
_main_station_order_cache: Dict[str, Tuple[List[str], float]] = {}


# ---------------------------------------------------------------------------
# Active project filtering (projectsdb.projects)
# ---------------------------------------------------------------------------

def get_active_project_databases() -> Optional[Set[str]]:
    """
    Fetch the set of database/schema names that are marked active in
    projectsdb.projects, so only active projects surface in monitoring.

    ASSUMPTION: projectsdb.projects has a `schemadb` column holding the schema
    name (matching what SHOW DATABASES returns) and a `status` column whose
    value indicates active/inactive (accepts 'active', 1, True - case
    insensitive - as active). If your actual column names differ, update the
    SELECT below accordingly.

    Returns:
        A set of lowercase active database names, or None if the lookup
        failed / the table is unavailable (callers should treat None as
        "don't filter" so a schema issue here never hides all projects).
    """
    cache_key = "active_projects"
    cached = _active_projects_cache.get(cache_key)
    if cached and (time.time() - cached[1] < _METADATA_CACHE_TTL):
        return cached[0]

    try:
        with get_db_connection() as cursor:
            cursor.execute("""
                SELECT schemadb, status
                FROM projectsdb.projects
            """)
            rows = cursor.fetchall()

            active = set()
            for schemadb, status in rows:
                if not schemadb:
                    continue
                status_str = str(status).strip().lower()
                if status_str in ('active', '1', 'true', 'yes'):
                    active.add(schemadb.strip().lower())

            _active_projects_cache[cache_key] = (active, time.time())
            return active
    except Exception as e:
        logger.error(f"Error fetching active projects from projectsdb.projects: {e}")
        return None


# ---------------------------------------------------------------------------
# Station common names (projectsdb.station)
# ---------------------------------------------------------------------------

def get_station_cname_map() -> Dict[Tuple[str, str, str], str]:
    """
    Fetch the full (customer, model, station) -> cname mapping from
    projectsdb.station. Keys are lowercased for case-insensitive matching.

    Returns:
        Dict keyed by (customer_lower, model_lower, station_lower) -> cname.
        Empty dict if the table is unavailable or has no rows.
    """
    cache_key = "station_cnames"
    cached = _station_cname_cache.get(cache_key)
    if cached and (time.time() - cached[1] < _METADATA_CACHE_TTL):
        return cached[0]

    mapping: Dict[Tuple[str, str, str], str] = {}
    try:
        with get_db_connection() as cursor:
            cursor.execute("""
                SELECT customer, model, station, cname
                FROM projectsdb.station
                WHERE cname IS NOT NULL AND cname != ''
            """)
            for customer, model, station, cname in cursor.fetchall():
                if not (customer and model and station and cname):
                    continue
                key = (customer.strip().lower(), model.strip().lower(), station.strip().lower())
                mapping[key] = cname

            _station_cname_cache[cache_key] = (mapping, time.time())
    except Exception as e:
        logger.error(f"Error fetching station common names from projectsdb.station: {e}")

    return mapping

def resolve_station_display_name(
    cname_map: Dict[Tuple[str, str, str], str],
    customer: str,
    model: str,
    station: str
) -> str:
    """Look up the common name for a station, falling back to the raw station code."""
    key = (customer.strip().lower(), model.strip().lower(), station.strip().lower())
    return cname_map.get(key, station)


# ---------------------------------------------------------------------------
# Station ordering based on the model's `_main` table
# ---------------------------------------------------------------------------

def get_main_station_order(database: str, model: str) -> List[str]:
    """
    Determine the correct station sequence for a customer+model based on
    the `{model}_main` table, instead of sorting stations alphabetically.

    STRATEGY: `_main` tracks every unit as it moves through the full station
    sequence for that model. We find the table's date/time column (same
    dynamic discovery ProductionDataProcessor already uses for the
    per-station tables) and take MIN(date_column) per distinct station,
    ordered ascending. Since units generally reach station 1 before station
    2, before station 3, etc., the earliest-seen station is first in the
    process, which recovers the real step order without needing an explicit
    sequence column.

    If `{model}_main` doesn't exist or has no usable date column, returns [].
    Callers should treat an empty result as "no ordering info available" and
    fall back to whatever order they already have.
    """
    cache_key = f"{database}::{model}"
    cached = _main_station_order_cache.get(cache_key)
    if cached and (time.time() - cached[1] < _METADATA_CACHE_TTL):
        return cached[0]

    main_table = f"{model}_main"
    order: List[str] = []
    try:
        with get_db_connection() as cursor:
            columns_info = ProductionDataProcessor.get_table_columns(cursor, database, main_table)

            column_names = [col[0] for col in columns_info]
            column_names_lc = {c.lower() for c in column_names}

            # Preferred path: explicit `station` column exists (one row per station event)
            if 'station' in column_names_lc:
                date_column = ProductionDataProcessor.find_date_column(columns_info)
                if not date_column:
                    logger.warning(f"{database}.{main_table} has no date/time column; cannot derive order")
                    return []

                cursor.execute(f"""
                    SELECT `station`, MIN(`{date_column}`) as first_seen
                    FROM `{database}`.`{main_table}`
                    WHERE `station` IS NOT NULL AND `station` != ''
                    GROUP BY `station`
                    ORDER BY first_seen ASC
                """)
                order = [row[0] for row in cursor.fetchall()]
            else:
                # Fallback: some schemas use a pivoted `_main` where each station
                # is a column header. In that case, derive station sequence from
                # the column ordering, excluding obvious metadata/date fields.
                date_column = ProductionDataProcessor.find_date_column(columns_info)

                exclude_names = {
                    'serial_num', 'serial', 'operator_en', 'operator', 'status',
                    'id', 'pk', 'created_at', 'updated_at', 'timestamp',
                    'date', 'time', 'system'
                }
                if date_column:
                    exclude_names.add(date_column.lower())

                candidate_stations = []
                for col in column_names:
                    col_lc = col.strip().lower()
                    if col_lc in exclude_names:
                        continue
                    # ignore index/auxiliary columns
                    if col_lc.startswith('idx_') or col_lc.startswith('is_'):
                        continue
                    candidate_stations.append(col)

                if not candidate_stations:
                    logger.warning(f"{database}.{main_table} has no `station` column and no candidate station columns; cannot derive order")
                    return []

                logger.info(f"Derived station order from {database}.{main_table} column headers: {candidate_stations}")
                order = candidate_stations

            _main_station_order_cache[cache_key] = (order, time.time())
    except Exception as e:
        # Common/expected case: no `_main` table for this model - not an error worth alarming on.
        logger.info(f"Could not derive station order from {database}.{main_table}: {e}")
        return []

    return order


def sort_stations_by_main_order(database: str, model: str, stations: List[str]) -> List[str]:
    """
    Sort a list of station codes according to their position in the model's
    `_main` sequence. Stations not found in `_main` (e.g. new/renamed
    stations) are appended at the end, in their original relative order,
    rather than being dropped.
    """
    main_order = get_main_station_order(database, model)
    if not main_order:
        return stations  # No ordering info - leave as-is rather than guessing.

    order_index = {s.strip().lower(): i for i, s in enumerate(main_order)}
    known = [s for s in stations if s.strip().lower() in order_index]
    unknown = [s for s in stations if s.strip().lower() not in order_index]
    known.sort(key=lambda s: order_index[s.strip().lower()])
    return known + unknown


def get_ordered_stations_for_model(customer: str, model: Optional[str] = None) -> Dict[str, List[Dict[str, str]]]:
    """
    Used by the /api/get-models-stations endpoint (and anywhere else that
    needs to populate the model/station filter dropdowns). Returns models
    and, if a model is given, that model's stations in `_main` sequence
    order with their display (cname) names attached.

    Returns:
        {
          "models": [<model codes present for this customer>],
          "stations": [{"code": <raw station>, "name": <display name>}, ...]
                      (empty list if no model was given)
        }
    """
    databases, tables_by_db = get_databases_and_tables()
    customer_lower = customer.strip().lower()

    matching_db = next((db for db in databases if db.lower() == customer_lower), None)
    if not matching_db:
        return {"models": [], "stations": []}

    tables = tables_by_db.get(matching_db, [])
    models_seen = []
    stations_for_model = []

    cname_map = get_station_cname_map()

    for table in tables:
        if table.endswith('_main'):
            continue  # `_main` isn't a real station table, skip it for listings
        parts = table.split('_', 1)
        table_model = parts[0]
        table_station = parts[1] if len(parts) == 2 else ''

        if table_model not in models_seen:
            models_seen.append(table_model)

        if model and table_model.lower() == model.strip().lower() and table_station:
            stations_for_model.append(table_station)

    if model:
        stations_for_model = sort_stations_by_main_order(matching_db, model, stations_for_model)
        station_payload = [
            {
                "code": s,
                "name": resolve_station_display_name(cname_map, matching_db, model, s)
            }
            for s in stations_for_model
        ]
    else:
        station_payload = []

    return {"models": models_seen, "stations": station_payload}


# ---------------------------------------------------------------------------
# Operator names (unchanged from previous version)
# ---------------------------------------------------------------------------

def get_operator_name(cursor, operator_en: str) -> str:
    """
    Get employee name from operators.main table

    Args:
        cursor: Database cursor
        operator_en: Operator code (e.g., 'KE0447')

    Returns:
        Employee name or operator code if not found
    """
    try:
        cursor.execute("""
            SELECT employee_name 
            FROM operators.main 
            WHERE operator_en = %s
            LIMIT 1
        """, (operator_en,))
        result = cursor.fetchone()
        return result[0] if result else operator_en
    except Exception as e:
        logger.warning(f"Could not fetch name for {operator_en}: {e}")
        return operator_en


def get_operator_names_batch(cursor, operator_codes: List[str]) -> Dict[str, str]:
    """
    Get employee names for multiple operators in one query

    Args:
        cursor: Database cursor
        operator_codes: List of operator codes

    Returns:
        Dictionary mapping operator_en to employee_name
    """
    if not operator_codes:
        return {}

    try:
        placeholders = ','.join(['%s'] * len(operator_codes))
        cursor.execute(f"""
            SELECT operator_en, employee_name 
            FROM operators.main 
            WHERE operator_en IN ({placeholders})
        """, tuple(operator_codes))

        # Create mapping dict, use operator_en as fallback if name not found
        name_map = {code: code for code in operator_codes}
        for operator_en, employee_name in cursor.fetchall():
            if employee_name:
                name_map[operator_en] = employee_name

        return name_map
    except Exception as e:
        logger.error(f"Error fetching operator names: {e}")
        # Return dict with codes as fallback
        return {code: code for code in operator_codes}


def get_active_operators(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Set[str]:
    """Get operators who have break_logs entries (returns operator codes)"""
    try:
        with get_db_connection() as cursor:
            if start_date and end_date:
                start_dt, end_dt = get_production_date_range(start_date, end_date)
            else:
                start_dt, end_dt = get_production_start_time()

            adjusted_start = start_dt - timedelta(hours=UTC_OFFSET_HOURS)
            adjusted_end = end_dt - timedelta(hours=UTC_OFFSET_HOURS)

            cursor.execute("""
                SELECT DISTINCT operator_en
                FROM projectsdb.break_logs
                WHERE timestamp BETWEEN %s AND %s
            """, (adjusted_start, adjusted_end))

            return {row[0] for row in cursor.fetchall() if row[0]}
    except Exception as e:
        logger.error(f"Error fetching active operators: {e}")
        return set()


def process_table_data(
    database: str,
    table: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    cname_map: Optional[Dict[Tuple[str, str, str], str]] = None
) -> List[ProductionRecord]:
    """Process data from a single table"""
    try:
        with get_db_connection() as cursor:
            columns_info = ProductionDataProcessor.get_table_columns(cursor, database, table)

            if not ProductionDataProcessor.validate_required_columns(columns_info):
                return []

            date_column = ProductionDataProcessor.find_date_column(columns_info)
            if not date_column:
                return []

            # Get date range
            if start_date and end_date:
                start_dt, end_dt = get_production_date_range(start_date, end_date)
            else:
                start_dt, end_dt = get_production_start_time()

            # Parse table name
            parts = table.split('_', 1)
            model = parts[0]
            station = parts[1] if len(parts) == 2 else ''

            # Query production data
            query = f"""
                SELECT 
                    operator_en,
                    COUNT(DISTINCT serial_num) as output,
                    MIN(`{date_column}`) as first_time,
                    MAX(`{date_column}`) as last_time
                FROM `{database}`.`{table}`
                WHERE `{date_column}` BETWEEN %s AND %s AND `status` = 1
                GROUP BY operator_en
                HAVING output > 0
            """

            cursor.execute(query, (start_dt, end_dt))
            rows = cursor.fetchall()

            if not rows:
                return []

            # Batch fetch operator names
            operator_codes = [row[0] for row in rows]
            operator_names = get_operator_names_batch(cursor, operator_codes)

            results = []
            adjusted_start = start_dt - timedelta(hours=UTC_OFFSET_HOURS)
            adjusted_end = end_dt - timedelta(hours=UTC_OFFSET_HOURS)

            # Batch fetch break logs
            placeholders = ','.join(['%s'] * len(operator_codes))

            cursor.execute(f"""
                SELECT operator_en, timestamp, action_type
                FROM projectsdb.break_logs
                WHERE operator_en IN ({placeholders})
                AND timestamp BETWEEN %s AND %s
                ORDER BY operator_en, timestamp ASC
            """, tuple(operator_codes) + (adjusted_start, adjusted_end))

            # Group logs by operator
            logs_by_operator = {}
            for operator_en, timestamp, action_type in cursor.fetchall():
                if operator_en not in logs_by_operator:
                    logs_by_operator[operator_en] = []
                logs_by_operator[operator_en].append((timestamp, action_type))

            # Resolve display name for this customer/model/station once per table
            display_station = station
            if cname_map is not None and station:
                display_station = resolve_station_display_name(cname_map, database, model, station)

            # Process each operator
            for operator_en, output, first_time, last_time in rows:
                logs = logs_by_operator.get(operator_en, [])

                # Fetch serial numbers
                cursor.execute(f"""
                    SELECT serial_num
                    FROM `{database}`.`{table}`
                    WHERE `{date_column}` BETWEEN %s AND %s 
                    AND operator_en = %s AND `status` = 1
                    ORDER BY `{date_column}`
                """, (start_dt, end_dt, operator_en))
                serial_nums = [r[0] for r in cursor.fetchall()]

                # Calculate cycle time
                cycle_time, start_time, end_time = calculate_cycle_time(
                    logs, first_time, last_time, output, model, station
                )

                # Get target and status
                target_time = ProductionDataProcessor.get_target_time(cursor, database, model, station)
                status = determine_status(cycle_time, target_time)

                # Get employee name
                employee_name = operator_names.get(operator_en, operator_en)

                results.append(ProductionRecord(
                    customer=database,
                    model=model,
                    station=station,
                    operator=employee_name,  # Use employee name instead of code
                    operator_code=operator_en,  # Keep code for reference
                    output=output,
                    target_time=target_time,
                    cycle_time=cycle_time,
                    start_time=start_time,
                    end_time=end_time,
                    status=status,
                    serial_nums=serial_nums,
                    duration_hours=(end_time - start_time).total_seconds() / 3600 if start_time and end_time else 0,
                    individual_durations=[cycle_time] if cycle_time > 0 else [],
                    station_display=display_station
                ))

            return results

    except Exception as e:
        logger.error(f"Error processing {database}.{table}: {e}")
        return []


def calculate_cycle_time(logs, first_time, last_time, output, model, station):
    """Calculate cycle time from break logs or production data"""
    if not logs:
        # No break logs - use production data
        duration = (last_time - first_time).total_seconds()
        cycle_time = duration / output if output > 0 and duration > 0 else 0
        return cycle_time, first_time, last_time

    # Filter logs to production timeframe
    station_start = first_time - timedelta(minutes=30) - timedelta(hours=UTC_OFFSET_HOURS)
    station_end = last_time + timedelta(minutes=30) - timedelta(hours=UTC_OFFSET_HOURS)
    relevant_logs = [(ts, action) for ts, action in logs if station_start <= ts <= station_end]

    if not relevant_logs:
        duration = (last_time - first_time).total_seconds()
        cycle_time = duration / output if output > 0 and duration > 0 else 0
        return cycle_time, first_time, last_time

    # Process break logs
    total_active = 0
    start_log = None
    first_start = None
    last_stop = None

    for ts, action in relevant_logs:
        local_ts = ts + timedelta(hours=UTC_OFFSET_HOURS)

        if action.lower() == "start":
            start_log = local_ts
            if first_start is None:
                first_start = local_ts
        elif action.lower() == "stop" and start_log:
            duration = (local_ts - start_log).total_seconds()
            if duration > 0:
                total_active += duration
            last_stop = local_ts
            start_log = None

    # Handle unclosed session
    if start_log:
        duration = (last_time - start_log).total_seconds()
        if duration > 0:
            total_active += duration
        last_stop = last_time

    cycle_time = total_active / output if output > 0 and total_active > 0 else 0
    start_time = first_start if first_start else first_time
    end_time = last_stop if last_stop else last_time

    return cycle_time, start_time, end_time


def determine_status(cycle_time, target_time):
    """Determine production status"""
    if target_time is None:
        return "ON TARGET" if cycle_time > 0 else "NO TARGET"

    orange_threshold = target_time * 1.2
    if cycle_time <= target_time:
        return "ON TARGET"
    elif cycle_time <= orange_threshold:
        return "ORANGE TARGET"
    else:
        return "BELOW TARGET"


@lru_cache(maxsize=128)
def get_databases_and_tables() -> Tuple[List[str], Dict[str, List[str]]]:
    """
    Get all databases and their tables (cached).

    NEW: databases are now also filtered against projectsdb.projects's active
    list (see get_active_project_databases). If that lookup fails for any
    reason, we fall back to the old hidden_databases-only behavior so a
    projectsdb.projects outage never blanks out the whole dashboard.
    """
    try:
        with get_db_connection() as cursor:
            cursor.execute("SHOW DATABASES")
            all_dbs = [db[0] for db in cursor.fetchall()
                       if db[0] not in db_config.hidden_databases]

            active_project_dbs = get_active_project_databases()
            if active_project_dbs is not None:
                databases = [db for db in all_dbs if db.lower() in active_project_dbs]
                if not databases:
                    # Nothing matched - most likely a naming mismatch between
                    # projectsdb.projects.schemadb and the real schema names.
                    # Fail open rather than showing an empty dashboard.
                    logger.warning(
                        "No databases matched projectsdb.projects active list; "
                        "showing all non-hidden databases instead."
                    )
                    databases = all_dbs
            else:
                databases = all_dbs

            tables_by_db = {}
            for db in databases:
                try:
                    cursor.execute(f"SHOW TABLES FROM `{db}`")
                    tables_by_db[db] = [tbl[0] for tbl in cursor.fetchall()]
                except Exception as e:
                    logger.warning(f"Could not access {db}: {e}")
                    tables_by_db[db] = []

            return databases, tables_by_db
    except Exception as e:
        logger.error(f"Error getting databases: {e}")
        return [], {}


_cache = {}
_cache_timeout = 60

def fetch_production_data(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    use_cache: bool = True
) -> Tuple[List[ProductionRecord], str, List[str], List[str], List[str]]:
    """Fetch all production data"""
    cache_key = f"{start_date}_{end_date}"

    if use_cache and cache_key in _cache:
        data, cached_time = _cache[cache_key]
        if time.time() - cached_time < _cache_timeout:
            return data

    databases, tables_by_db = get_databases_and_tables()
    cname_map = get_station_cname_map()
    all_records = []
    active_databases = set()
    models = set()
    stations = set()

    # Prepare tasks
    tasks = [(db, table) for db, tables in tables_by_db.items() for table in tables]

    # Process in parallel
    max_workers = min(len(tasks), db_config.pool_size, 20) if tasks else 1
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(process_table_data, db, table, start_date, end_date, cname_map): (db, table)
            for db, table in tasks
        }

        for future in as_completed(future_to_task):
            try:
                results = future.result(timeout=45)
                if results:
                    with lock:
                        all_records.extend(results)
                        for record in results:
                            active_databases.add(record.customer.lower())
                            models.add(record.model)
                            stations.add(record.station)
            except Exception as e:
                db, table = future_to_task[future]
                logger.error(f"Error processing {db}.{table}: {e}")

    # Order records by (customer, model) FIRST so every product's rows stay
    # contiguous as one block, then by the model's _main station sequence
    # within that block. Without grouping by product first, two products
    # whose stations happen to share the same order index could end up
    # interleaved instead of appearing as clean step-by-step blocks.
    order_cache: Dict[Tuple[str, str], Dict[str, int]] = {}

    def _station_sort_key(record: ProductionRecord) -> Tuple[str, str, int]:
        cache_k = (record.customer, record.model)
        if cache_k not in order_cache:
            main_order = get_main_station_order(record.customer, record.model)
            order_cache[cache_k] = {s.strip().lower(): i for i, s in enumerate(main_order)}
        idx_map = order_cache[cache_k]
        station_idx = idx_map.get(record.station.strip().lower(), len(idx_map) + 1)
        return (record.customer.lower(), record.model.lower(), station_idx)

    all_records.sort(key=_station_sort_key)

    # Date display
    if start_date and end_date:
        date_display = f"{start_date} → {end_date}"
    else:
        start_dt, _ = get_production_start_time()
        date_display = start_dt.strftime('%Y-%m-%d')

    result = (all_records, date_display, sorted(active_databases), sorted(models), sorted(stations))

    if use_cache:
        _cache[cache_key] = (result, time.time())

    return result


def apply_filters(
    records: List[ProductionRecord],
    filters: Dict[str, Optional[str]],
    active_operators: Optional[Set[str]] = None
) -> List[ProductionRecord]:
    """Apply filters to records"""
    filtered = records

    if filters.get("customer"):
        filtered = [r for r in filtered if r.customer.lower() == filters["customer"].lower()]
    if filters.get("model"):
        filtered = [r for r in filtered if r.model.lower() == filters["model"].lower()]
    if filters.get("station"):
        # Match against either the raw station code or its display/common name,
        # since the dropdown may now be populated with cnames.
        wanted = filters["station"].lower()
        filtered = [
            r for r in filtered
            if r.station.lower() == wanted or (r.station_display or '').lower() == wanted
        ]
    if active_operators is not None:
        # Filter by operator_code since active_operators contains codes
        filtered = [r for r in filtered if r.operator_code in active_operators]

    return filtered


def get_operator_statistics(records: List[ProductionRecord], operator_name: str) -> Dict:
    """Calculate statistics for a specific operator (by name or code)"""
    # Try to match by name first, then by code
    filtered = [r for r in records if r.operator == operator_name or r.operator_code == operator_name]

    all_serials = []
    total_output = 0
    total_duration = 0

    for record in filtered:
        all_serials.extend(record.serial_nums)
        total_output += record.output
        if record.duration_hours:
            total_duration += record.duration_hours

    return {
        "serials": all_serials,
        "total_output": total_output,
        "total_duration_hours": round(total_duration, 2),
        "mode_cycle_time": None,
        "stations_count": len(set(f"{r.customer}-{r.model}-{r.station}" for r in filtered)),
        "records": filtered
    }