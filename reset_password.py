import mysql.connector
import bcrypt
import toml 

# --- CONFIGURATION ---
# The username you want to reset
USERNAME_TO_RESET = "superuser_admin" # <-- This is the user you confirmed exists
# The new password you want to set
NEW_PASSWORD = "Vivek@143"    # <-- Set your desired new password here

# --- SCRIPT LOGIC ---
print("--- Password Reset Script ---")

try:
    # Load database credentials from your secrets file
    with open(".streamlit/secrets.toml", "r") as f:
        secrets = toml.load(f)
    db_creds = secrets.get("mysql")

    if not db_creds:
        print("ERROR: Could not find [mysql] credentials in .streamlit/secrets.toml")
        exit()

    print(f"Attempting to connect to database '{db_creds.get('database')}' with user '{db_creds.get('user')}'...")
    conn = mysql.connector.connect(**db_creds)
    cursor = conn.cursor()
    print("Database connection successful.")

    # Hash the new password
    hashed_password = bcrypt.hashpw(NEW_PASSWORD.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    print(f"Generated new hashed password for user '{USERNAME_TO_RESET}'.")

    # Update the user's password in the database
    sql_query = "UPDATE users SET hashed_password = %s WHERE username = %s"
    cursor.execute(sql_query, (hashed_password, USERNAME_TO_RESET))
    conn.commit()

    if cursor.rowcount > 0:
        print(f"\nSUCCESS: Password for user '{USERNAME_TO_RESET}' has been updated.")
    else:
        print(f"\nWARNING: No user with the username '{USERNAME_TO_RESET}' was found. No changes were made.")

except FileNotFoundError:
    print("ERROR: secrets.toml file not found. Make sure you are running this script from your project's root directory.")
except mysql.connector.Error as err:
    print(f"A database error occurred: {err}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
        print("Database connection closed.")
