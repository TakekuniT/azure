import pyodbc

# active directory authentication
# conn = pyodbc.connect(
#     "Driver={ODBC Driver 18 for SQL Server};"
#     "Server=tcp:sfas-observations.database.windows.net,1433;"
#     "Database=flood_map_data;"
#     "Authentication=ActiveDirectoryInteractive;"
#     "UID=ttanemor@stevens.edu;"
#     "Encrypt=yes;"
#     "TrustServerCertificate=no;"
# )



# server = 'sfas-observations.database.windows.net'
# database = 'flood_map_data'
# username = 'newuser'                 
# password = 'StrongPassword123!'     
# driver = '{ODBC Driver 18 for SQL Server}'

# conn = pyodbc.connect(
#     f"DRIVER={driver};"
#     f"SERVER=tcp:{server},1433;"
#     f"DATABASE={database};"
#     f"UID={username};"
#     f"PWD={password};"
#     "Encrypt=yes;"
#     "TrustServerCertificate=no;"
# )

# cursor = conn.cursor()

# print("CONNECTED!")

import pyodbc

server = "sfas-observations.database.windows.net"
database = "flood_map_data"
driver = "{ODBC Driver 18 for SQL Server}"
username = "ttanemor@stevens.edu"

conn = pyodbc.connect(
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    "Authentication=ActiveDirectoryInteractive;"
    f"UID={username};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)

cursor = conn.cursor()
cursor.execute("SELECT TOP 5 * FROM INFORMATION_SCHEMA.TABLES")
for row in cursor.fetchall():
    print(row)