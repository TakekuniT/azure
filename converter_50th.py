import json
import pyodbc
import sys
from shapely.geometry import shape

# Configuration for Azure SQL
# Use the credentials you were trying to use in the portal screenshot
# Configuration for Azure SQL
db_config = {
    'driver': '{ODBC Driver 18 for SQL Server}',
    'server': 'sfas-observations.database.windows.net',
    'database': 'flood_map_data',
    'Authentication': 'ActiveDirectoryInteractive',
    'user': 'ttanemor@stevens.edu', 
    'encrypt': 'yes',
    'trust_server_certificate': 'no',
}

# Azure SQL Syntax: Use IDENTITY(1,1) instead of SERIAL
create_flood_table_query = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Flood_Map_Data') AND type in (N'U'))
    BEGIN
        CREATE TABLE Flood_Map_Data (
            id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
            polygon_id INTEGER NOT NULL,
            time DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
            flooded INTEGER NOT NULL,
            depth_class INTEGER NOT NULL,
            geometry GEOMETRY NOT NULL                             
        );
    END
"""

create_depth_table_query = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Flood_Depth_Data') AND type in (N'U'))
    BEGIN
        CREATE TABLE Flood_Depth_Data (
            id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
            depth_class INTEGER NOT NULL UNIQUE,
            depth_min INTEGER,
            depth_max INTEGER
        )
    END
"""

def get_conn():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=sfas-observations.database.windows.net,1433;" # Explicit port
        "DATABASE=flood_map_data;"
        "UID=ttanemor@stevens.edu;" 
        "AUTHENTICATION=ActiveDirectoryInteractive;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "LoginTimeout=60;" # Increased timeout
    )
    return pyodbc.connect(conn_str)

def import_flood_geojson(file_path):
    conn = get_conn()
    cur = conn.cursor()
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # 1. Handle Depth Classes
        depth_classes = {}
        for feature in data['features']:
            props = feature.get('properties', {})
            d_class = props.get('depth_class')
            if d_class is not None and d_class not in depth_classes:
                depth_classes[d_class] = (props.get('depth_min_m'), props.get('depth_max_m'))
        
        for d_class, (d_min, d_max) in depth_classes.items():
            # Azure SQL "ON CONFLICT" equivalent is a manual check or MERGE
            cur.execute("""
                IF NOT EXISTS (SELECT 1 FROM Flood_Depth_Data WHERE depth_class = ?)
                INSERT INTO Flood_Depth_Data (depth_class, depth_min, depth_max) VALUES (?, ?, ?)
            """, (d_class, d_class, d_min, d_max))

        # 2. Process Features
        for i, feature in enumerate(data['features'], 1):
            props = feature.get('properties', {})
            geom_raw = feature.get('geometry')
            
            if not geom_raw: continue

            # Azure SQL doesn't have a native "ST_GeomFromGeoJSON"
            # We convert GeoJSON to WKT (Well-Known Text) using Shapely
            wkt_geom = shape(geom_raw).wkt
            
            # Use STGeomFromText with SRID 4326 (WGS84)
            cur.execute("""
                INSERT INTO Flood_Map_Data (flooded, depth_class, polygon_id, geometry) 
                VALUES (?, ?, ?, geometry::STGeomFromText(?, 4326))
            """, (props.get('flooded', 0), props.get('depth_class'), 
                  props.get('PolygonID', i), wkt_geom))
        
        conn.commit()
        print("Import Successful")
    finally:
        conn.close()

if __name__ == "__main__":
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(create_flood_table_query)
    cur.execute(create_depth_table_query)
    conn.commit()
    conn.close()
    
    import_flood_geojson("flood_depth_bbaynorth_50th.geojson")


'''
Key Changes Explained:

Driver Change: We switched from psycopg2 (Postgres) to pyodbc (SQL Server).

Table Creation: Azure SQL doesn't support CREATE TABLE IF NOT EXISTS. Instead, we use the IF NOT EXISTS (SELECT...) block.

Geometry Handling: PostGIS has ST_GeomFromGeoJSON. Azure SQL does not.

Solution: I added a step using the shapely library to convert the GeoJSON geometry into WKT (Well-Known Text). You can install it via pip install shapely.

The SQL then uses geometry::STGeomFromText(?, 4326).

Identity Columns: Changed SERIAL to INT IDENTITY(1,1).

Timestamps: Changed TIMESTAMP WITH TIME ZONE to DATETIMEOFFSET (Azure's standard for time-zone aware dates).
'''