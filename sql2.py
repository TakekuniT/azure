import json
import re
from shapely.geometry import shape

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

def generate_sql_commands(FILENAME, OUTPUT_SQL):
    try:
        percentile = extract_percentile(FILENAME)

        # ONE table per percentile
        flood_table = f"Flood_Map_Data_{percentile}"

        print(f"Opening {FILENAME}...")
        with open(FILENAME, 'r') as f:
            data = json.load(f)

        print(f"Processing {len(data['features'])} features...")

        with open(OUTPUT_SQL, "w") as out:
            # Table creation
            create_flood_table = f"""
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{flood_table}') AND type in (N'U'))
BEGIN
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
END
"""
            out.write("-- Table Setup\n")
            out.write(create_flood_table + ";\nGO\n\n")
            out.write("-- Populating Flood Map Data\n")

            # Insert polygons
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

                sql_line = (
                    f"INSERT INTO {flood_table} "
                    f"(polygon_id, flooded, depth_class, depth_min, depth_max, area_sq_km, geometry) "
                    f"VALUES ({poly_id}, {flooded}, {d_class}, "
                    f"{d_min if d_min is not None else 'NULL'}, "
                    f"{d_max if d_max is not None else 'NULL'}, "
                    f"{area if area is not None else 'NULL'}, "
                    f"geometry::STGeomFromText('{wkt_geom}', 4326));\n"
                )

                out.write(sql_line)

                if i % 100 == 0:
                    out.write("GO\n")

        print(f"Success! SQL script generated: {OUTPUT_SQL}")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    generate_sql_commands("sql_folder/flood_depth_bbaynorth_05th.geojson", "upload_data_05th.sql")
    generate_sql_commands("sql_folder/flood_depth_bbaynorth_50th.geojson", "upload_data_50th.sql")
    generate_sql_commands("sql_folder/flood_depth_bbaynorth_95th.geojson", "upload_data_95th.sql")