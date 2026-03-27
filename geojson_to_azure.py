import json
import re
from shapely.geometry import shape
import pyodbc

# -----------------------------
# Configuration
# -----------------------------
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

# -----------------------------
# Helper: extract percentile
# -----------------------------
def extract_percentile(filename):
    """
    Extracts percentile like '05', '50', '95' from filename.
    Example: flood_depth_bbaynorth_05th.geojson -> 05
    """
    match = re.search(r'_(\d{2})th', filename)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Could not extract percentile from {filename}")

# -----------------------------
# Main function: insert data
# -----------------------------
def upload_geojson_to_db(filename):
    try:
        percentile = extract_percentile(filename)
        flood_table = f"Flood_Map_Data_{percentile}"

        # Load GeoJSON
        print(f"Opening {filename}...")
        with open(filename, 'r') as f:
            data = json.load(f)

        print(f"Processing {len(data['features'])} features...")

        # Connect to Azure SQL
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Drop table if exists & create new
        cursor.execute(f"""
            IF OBJECT_ID(N'{flood_table}', N'U') IS NOT NULL
                DROP TABLE {flood_table};

            CREATE TABLE {flood_table} (
                id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
                polygon_id INTEGER NOT NULL,
                time DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
                flooded INTEGER NOT NULL,
                depth_class INTEGER NOT NULL,
                depth_min FLOAT NULL,
                depth_max FLOAT NULL,
                area_sq_km FLOAT NULL,
                geometry GEOMETRY NOT NULL
            );
        """)
        conn.commit()
        print(f"Table {flood_table} created.")

        # Insert each polygon
        for i, feature in enumerate(data['features'], 1):
            props = feature.get('properties', {})
            geom = feature.get('geometry')

            if not geom:
                continue

            wkt_geom = shape(geom).wkt

            flooded = props.get('flooded', 0)
            d_class = props.get('depth_class', 0)
            d_min = props.get('depth_min_m')
            d_max = props.get('depth_max_m')
            poly_id = props.get('polygon_id', i)
            area = props.get('area_sq_km')

            cursor.execute(f"""
                INSERT INTO {flood_table} 
                (polygon_id, flooded, depth_class, depth_min, depth_max, area_sq_km, geometry)
                VALUES (?, ?, ?, ?, ?, ?, geometry::STGeomFromText(?, 4326))
            """, poly_id, flooded, d_class,
                 d_min if d_min is not None else None,
                 d_max if d_max is not None else None,
                 area if area is not None else None,
                 wkt_geom)

            # Commit every 100 rows to avoid huge transaction
            if i % 100 == 0:
                conn.commit()

        conn.commit()
        conn.close()
        print(f"Upload complete for {flood_table}!")

    except Exception as e:
        print(f"Error: {e}")

# -----------------------------
# Run for all percentiles
# -----------------------------
if __name__ == "__main__":
    upload_geojson_to_db("geojson_folder/flood_depth_bbaynorth_05th.geojson")
    upload_geojson_to_db("geojson_folder/flood_depth_bbaynorth_50th.geojson")
    upload_geojson_to_db("geojson_folder/flood_depth_bbaynorth_95th.geojson")