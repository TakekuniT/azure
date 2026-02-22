import json
from shapely.geometry import shape

# Configuration
FILENAME = "flood_depth_bbaynorth_05th.geojson"
OUTPUT_SQL = "upload_data.sql"

# Azure SQL Syntax Schemas
CREATE_FLOOD_TABLE = """
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

CREATE_DEPTH_TABLE = """
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

def generate_sql_commands():
    try:
        print(f"Opening {FILENAME}...")
        with open(FILENAME, 'r') as f:
            data = json.load(f)

        print(f"Processing {len(data['features'])} features...")
        
        with open(OUTPUT_SQL, "w") as out:
            # 1. Write the table creation scripts
            out.write("-- Table Setup\n")
            out.write(CREATE_DEPTH_TABLE + ";\nGO\n")
            out.write(CREATE_FLOOD_TABLE + ";\nGO\n\n")
            
            # 2. Extract unique depth classes for the lookup table
            depth_classes = {}
            for feature in data['features']:
                props = feature.get('properties', {})
                d_class = props.get('depth_class')
                if d_class is not None and d_class not in depth_classes:
                    depth_classes[d_class] = (props.get('depth_min_m'), props.get('depth_max_m'))
            
            out.write("-- Populating Depth Lookup Table\n")
            for d_class, (d_min, d_max) in depth_classes.items():
                # Handling None values for SQL NULL
                d_min_val = d_min if d_min is not None else 'NULL'
                d_max_val = d_max if d_max is not None else 'NULL'
                
                out.write(f"IF NOT EXISTS (SELECT 1 FROM Flood_Depth_Data WHERE depth_class = {d_class}) ")
                out.write(f"INSERT INTO Flood_Depth_Data (depth_class, depth_min, depth_max) VALUES ({d_class}, {d_min_val}, {d_max_val});\n")
            
            out.write("\nGO\n-- Populating Flood Map Data\n")

            # 3. Process each polygon feature
            for i, feature in enumerate(data['features'], 1):
                props = feature.get('properties', {})
                geom = feature.get('geometry')
                
                if not geom:
                    continue
                
                # Convert GeoJSON geometry to Well-Known Text (WKT)
                wkt_geom = shape(geom).wkt
                
                flooded = props.get('flooded', 0)
                d_class = props.get('depth_class', 0)
                poly_id = props.get('PolygonID', i)
                
                # Format the INSERT statement for Azure SQL Geometry
                sql_line = (
                    f"INSERT INTO Flood_Map_Data (flooded, depth_class, polygon_id, geometry) "
                    f"VALUES ({flooded}, {d_class}, {poly_id}, geometry::STGeomFromText('{wkt_geom}', 4326));\n"
                )
                out.write(sql_line)
                
                if i % 100 == 0:
                    out.write("GO\n") 

        print(f"Success! SQL script generated: {OUTPUT_SQL}")
        print("Next Step: Copy the contents of this file into the Azure Query Editor.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    generate_sql_commands()