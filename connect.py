import pyodbc

conn = pyodbc.connect(
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=tcp:sfas-observations.database.windows.net,1433;"
    "Database=flood_map_data;"
    "Authentication=ActiveDirectoryDeviceCodeFlow;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)

print("CONNECTED!")