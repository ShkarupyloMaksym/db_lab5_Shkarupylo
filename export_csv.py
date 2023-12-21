import psycopg2
import pandas as pd
import os

db_params = {
    "host": "localhost",
    "database": "NobelPrizeWinners",
    "user": "Shkarupylo",
    "password": "Shkarupylo",
    "port": "5432",
}


def export_table_to_csv(conn, table_name):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)

    if not os.path.exists('csv_saved_files'):
        os.mkdir('csv_saved_files')

    df.to_csv(f"csv_saved_files/{table_name}.csv", index=False)


if __name__ == "__main__":
    try:
        conn = psycopg2.connect(**db_params)

        # Export each table to CSV
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public'")
        table_names = [table[0] for table in cur.fetchall()]
        print(f'tables for exporting: {table_names}')
        # table_names = ['laureate', 'organization', 'prize', 'prizelaureates']
        for table_name in table_names:
            export_table_to_csv(conn, table_name)

        print("CSV files exported successfully.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the database connection
        if conn:
            conn.close()
