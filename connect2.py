import  pyodbc
conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=sfas-observations.database.windows.net,1433;"
    "DATABASE=flood_map_data;"
    "UID=CloudSA483abf1a;"
    "PWD=Password123;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

def test():
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                polygon_id,
                flooded,
                depth_class,
                depth_min,
                depth_max,
                area_sq_km
            FROM Flood_Map_Data_05
        """)
        for row in cursor.fetchall():
            print(row)

        conn.close()

    except Exception as e:
        print("DB connection error:", e)

test()