import json
from shapely.geometry import shape

def generate_flood_sql(percentile: str):
    """
    Generates an Azure SQL script from a GeoJSON flood file for a given percentile.

    Args:
        percentile (str): Percentile string like '05', '50', or '95'.
    """
    # Construct file names and table names dynamically
    input_file = f"flood_depth_bbaynorth_{percentile}th.geojson"
    output_sql = f"upload_data_{percentile}th.sql"
    flood_table = f"Flood_Map_Data_{percentile}"
    depth_table = f"Flood_Depth_Data_{percentile}"

    # Azure SQL Syntax Schemas
    create_flood_table = f"""
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{flood_table}') AND type in (N'U'))
BEGIN
    CREATE TABLE {flood_table} (
        id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
        polygon_id INTEGER NOT NULL,
        time DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
        flooded INTEGER NOT NULL,
        depth_class INTEGER NOT NULL,
        geometry GEOMETRY NOT NULL                             
    );
END
"""

    create_depth_table = f"""
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{depth_table}') AND type in (N'U'))
BEGIN
    CREATE TABLE {depth_table} (
        id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
        depth_class INTEGER NOT NULL UNIQUE,
        depth_min INTEGER,
        depth_max INTEGER
    )
END
"""

    try:
        print(f"Opening {input_file}...")
        with open(input_file, 'r') as f:
            data = json.load(f)

        print(f"Processing {len(data['features'])} features...")
        
        with open(output_sql, "w") as out:
            # 1. Write the table creation scripts
            out.write("-- Table Setup\n")
            out.write(create_depth_table + ";\nGO\n")
            out.write(create_flood_table + ";\nGO\n\n")
            
            # 2. Extract unique depth classes for the lookup table
            depth_classes = {}
            for feature in data['features']:
                props = feature.get('properties', {})
                d_class = props.get('depth_class')
                if d_class is not None and d_class not in depth_classes:
                    depth_classes[d_class] = (props.get('depth_min_m'), props.get('depth_max_m'))
            
            out.write("-- Populating Depth Lookup Table\n")
            for d_class, (d_min, d_max) in depth_classes.items():
                d_min_val = d_min if d_min is not None else 'NULL'
                d_max_val = d_max if d_max is not None else 'NULL'
                
                out.write(f"IF NOT EXISTS (SELECT 1 FROM {depth_table} WHERE depth_class = {d_class}) ")
                out.write(f"INSERT INTO {depth_table} (depth_class, depth_min, depth_max) VALUES ({d_class}, {d_min_val}, {d_max_val});\n")
            
            out.write("\nGO\n-- Populating Flood Map Data\n")

            # 3. Process each polygon feature
            for i, feature in enumerate(data['features'], 1):
                props = feature.get('properties', {})
                geom = feature.get('geometry')
                
                if not geom:
                    continue
                
                # Convert GeoJSON geometry to WKT
                wkt_geom = shape(geom).wkt
                
                flooded = props.get('flooded', 0)
                d_class = props.get('depth_class', 0)
                poly_id = props.get('PolygonID', i)
                
                sql_line = (
                    f"INSERT INTO {flood_table} (flooded, depth_class, polygon_id, geometry) "
                    f"VALUES ({flooded}, {d_class}, {poly_id}, geometry::STGeomFromText('{wkt_geom}', 4326));\n"
                )
                out.write(sql_line)
                
                if i % 100 == 0:
                    out.write("GO\n") 

        print(f"✅ Success! SQL script generated: {output_sql}")
        print("Next Step: Copy the contents of this file into the Azure Query Editor.")

    except Exception as e:
        print(f"❌ Error: {e}")


# if __name__ == "__main__":
#     # Example usage
#     percentile = input("Enter percentile (e.g., 05, 50, 95): ").strip()
#     generate_flood_sql(percentile)

generate_flood_sql('05')
generate_flood_sql('50')
generate_flood_sql('95')