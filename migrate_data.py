import sqlite3
import mysql.connector
import streamlit as st
import json
import logging
import re

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def migrate_data_final():
    """
    Final, production-ready script to migrate data from SQLite to MySQL.
    This version includes a specific fix for NaN values in the JSON data.
    """
    SQLITE_DB_PATH = "validation_dashboard.db" 
    
    try:
        MYSQL_CREDS = st.secrets["mysql"]
    except Exception as e:
        logging.error("Could not read secrets.toml. Make sure it exists.")
        return

    logging.info("Starting final data migration from SQLite to MySQL...")
    sqlite_conn = None
    mysql_conn = None

    try:
        sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
        sqlite_conn.row_factory = sqlite3.Row
        mysql_conn = mysql.connector.connect(**MYSQL_CREDS)
        sqlite_cursor = sqlite_conn.cursor()
        mysql_cursor = mysql_conn.cursor()
        
        logging.info("Successfully connected to both databases.")

        tables_to_migrate = ["users", "validation_runs", "exceptions", "department_summary", "user_performance", "correction_status"]
        
        mysql_cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        logging.info("MySQL foreign key checks disabled.")

        logging.info("Emptying all destination tables in MySQL...")
        for table_name in reversed(tables_to_migrate):
            mysql_cursor.execute(f"TRUNCATE TABLE `{table_name}`;")
        logging.info("All destination tables are now empty.")

        for table_name in tables_to_migrate:
            logging.info(f"--- Migrating table: {table_name} ---")
            
            sqlite_cursor.execute(f"SELECT * FROM {table_name}")
            records = sqlite_cursor.fetchall()
            
            if not records:
                logging.info(f"Table '{table_name}' is empty. Skipping.")
                continue

            column_names = [desc[0] for desc in sqlite_cursor.description]
            quoted_column_names = [f"`{col}`" for col in column_names]
            placeholders = ", ".join(["%s"] * len(column_names))
            insert_query = f"INSERT INTO `{table_name}` ({', '.join(quoted_column_names)}) VALUES ({placeholders})"
            
            data_to_insert = []

            # --- FINAL FIX: Special handling for 'exceptions' table to replace NaN with null ---
            if table_name == 'exceptions':
                logging.info("Applying NaN -> null fix for 'exceptions' table...")
                try:
                    json_col_index = column_names.index('original_row_data')
                except ValueError:
                    json_col_index = -1

                for row in records:
                    row_list = list(row)
                    if json_col_index != -1 and isinstance(row_list[json_col_index], str):
                        # This regex replacement is safer. It replaces `NaN` when it appears as a value, not as part of a string.
                        # It looks for : NaN, [NaN, or {..., "key": NaN}
                        problem_text = row_list[json_col_index]
                        # A robust regex to replace NaN which is not inside quotes
                        # This finds NaN that is preceded by a colon, comma, or opening bracket, with optional whitespace
                        # and replaces it with the JSON standard 'null'.
                        clean_text = re.sub(r'([\[:,]\s*)NaN', r'\1null', problem_text)
                        row_list[json_col_index] = clean_text

                    data_to_insert.append(tuple(row_list))
            else:
                data_to_insert = [tuple(row) for row in records]
            
            mysql_cursor.executemany(insert_query, data_to_insert)
            logging.info(f"Successfully inserted {mysql_cursor.rowcount} records into MySQL table `{table_name}`.")

            mysql_cursor.execute(f"SELECT MAX(id) FROM `{table_name}`")
            max_id = mysql_cursor.fetchone()[0]
            if max_id:
                next_id = max_id + 1
                mysql_cursor.execute(f"ALTER TABLE `{table_name}` AUTO_INCREMENT = {next_id};")
                logging.info(f"AUTO_INCREMENT for `{table_name}` reset to {next_id}.")

        mysql_cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        logging.info("MySQL foreign key checks re-enabled.")
        
        mysql_conn.commit()
        logging.info("--- MIGRATION COMPLETED SUCCESSFULLY! ---")

    except Exception as e:
        logging.error(f"An error occurred during migration: {e}", exc_info=True)
        if mysql_conn:
            mysql_conn.rollback()
    finally:
        if sqlite_conn:
            sqlite_conn.close()
        if mysql_conn and mysql_conn.is_connected():
            mysql_conn.close()
        logging.info("Database connections closed.")

if __name__ == "__main__":
    migrate_data_final()