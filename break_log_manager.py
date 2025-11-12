from datetime import datetime

class BreakLogManager:
    def __init__(self, db):
        self.db = db

    def handle_unclosed_sessions(self, operator_name):
        """
        Check if the operator has any unclosed session (missing end time).
        If yes, close it automatically with the current time.
        """
        cursor = self.db.cursor(dictionary=True)

        # Find the most recent unclosed session
        cursor.execute("""
            SELECT * FROM break_logs
            WHERE operator_name = %s AND end_time IS NULL
            ORDER BY start_time DESC LIMIT 1
        """, (operator_name,))
        unclosed = cursor.fetchone()

        if unclosed:
            now = datetime.now()
            cursor.execute("""
                UPDATE break_logs
                SET end_time = %s,
                    status = 'AUTO-CLOSED',
                    remarks = 'Session auto-ended due to missing stop event.'
                WHERE id = %s
            """, (now, unclosed['id']))
            self.db.commit()

            print(f"⚠️ Auto-closed unended session for {operator_name} at {now}.")

        cursor.close()

    def start_session(self, operator_name):
        """Start a new session and handle any previous unclosed one."""
        self.handle_unclosed_sessions(operator_name)

        now = datetime.now()
        cursor = self.db.cursor()
        cursor.execute("""
            INSERT INTO break_logs (operator_name, start_time, status)
            VALUES (%s, %s, %s)
        """, (operator_name, now, 'IN PROGRESS'))
        self.db.commit()
        cursor.close()

        print(f"✅ New session started for {operator_name} at {now}.")

    def stop_session(self, operator_name):
        """End the operator's current active session."""
        now = datetime.now()
        cursor = self.db.cursor()
        cursor.execute("""
            UPDATE break_logs
            SET end_time = %s, status = 'COMPLETED'
            WHERE operator_name = %s AND end_time IS NULL
        """, (now, operator_name))
        self.db.commit()
        cursor.close()

        print(f"🕓 Session stopped for {operator_name} at {now}.")
