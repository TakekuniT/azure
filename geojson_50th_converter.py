import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
import sys


filename = r"flood_depth_bbaynorth_50th.geojson"
create_flood_table_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS Flood_Map_Data (
        id SERIAL PRIMARY KEY NOT NULL,
        polygon_id INTEGER NOT NULL,
        time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        flooded INTEGER NOT NULL,
        depth_class INTEGER NOT NULL,
        geometry GEOMETRY(Polygon, 4326) NOT NULL                             
    );
""")

create_depth_table_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS Flood_Depth_Data (
        id SERIAL PRIMARY KEY NOT NULL,
        depth_class INTEGER NOT NULL UNIQUE,
        depth_min INTEGER,
        depth_max INTEGER
    )
""")

def create_depth_table(conn):
    try:
        cur = conn.cursor()
        cur.execute(create_depth_table_query)
        conn.commit()
    except Exception as e:
        print(f"Error creating Flood_Depth_Data table: {e}")
        conn.rollback()
    finally:
        cur.close()

def create_geojson_table(conn):
    #first check if it alr exists, if not create it
    #calling the table Flood_Map_Data
    try:
        cur = conn.cursor()
        cur.execute(create_flood_table_query)
        conn.commit()
    except Exception as e:
        print(f"Error creating Flood_Map_Data table: {e}")
        conn.rollback()
    finally:
        cur.close()

def import_flood_geojson(file_path, db_params):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        #for now i j wanna see the properties of the first feature
        total_features = len(data['features'])
        print(f"Found {total_features} polygon features")
        print(f"Properties: {data['features'][0]['properties']}")
        imported_count = 0
        error_count = 0

        #Dict to store depth class mappings
        depth_classes = {}
        #get unique depth classes and their min/max values
        for feature in data['features']:
            properties = feature.get('properties', {})
            depth_class = properties.get('depth_class')
            depth_min = properties.get('depth_min_m')
            depth_max = properties.get('depth_max_m')
            if depth_class is not None and depth_class not in depth_classes:
                depth_classes[depth_class] = (depth_min, depth_max)
        
        #then insert unique depth classes into the Flood_Depth_Data table
        for depth_class, (depth_min, depth_max) in depth_classes.items():
            try:
                cur.execute("""INSERT INTO Flood_Depth_Data (depth_class, depth_min, depth_max) 
                            VALUES (%s, %s, %s) ON CONFLICT (depth_class) DO NOTHING""",
                            (depth_class, depth_min, depth_max))
            except Exception as e:
                print(f"Error inserting depth class {depth_class}: {e}")
                continue
        print(f"Inserted {len(depth_classes)} unique depth classes into Flood_Depth_Data table.")

        #process each polygon feature
        for i, feature in enumerate(data['features'], 1):
            try:
                properties = feature.get('properties', {})
                geometry = feature.get('geometry')
                flooded = properties.get('flooded', 0)
                polygon_id = properties.get('PolygonID')
                depth_class = properties.get('depth_class')

                #make sure geometry is valid
                if geometry is None:
                    print(f"Warning: Feature {i} has no geometry, skipping.")
                    error_count += 1
                    continue
                if polygon_id is None:
                    print(f"Warning: Feature {i} has no PolygonID, using index.")
                    polygon_id = i
                
                #convert geo to json for postgis
                geometry_json = json.dumps(geometry)
                #insert into db
                cur.execute("""INSERT INTO Flood_Map_Data (flooded, depth_class, polygon_id, geometry) 
                            VALUES (%s, %s, %s, ST_GeomFromGeoJSON(%s))""",
                            (flooded, depth_class, polygon_id, geometry_json))
            
                imported_count += 1

            except Exception as e:
                print(f"Error processing feature {i}: {e}")
                error_count += 1
                continue
        conn.commit()
        print(f"Import complete: {imported_count} features imported, {error_count} errors.")
        return imported_count
    except Exception as e:
        print(f"Error during import: {e}")
        conn.rollback()
        raise 
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    db_config = {
        'database': 'postgres',
        'user': 'postgres',
        'password': 'postgres'
    }
    geojson_file = filename
    try:
        print("Creating tables if they do not exist...")
        conn = psycopg2.connect(**db_config) # type: ignore
        create_depth_table(conn)
        create_geojson_table(conn)
        conn.close()
        print(f"Importing data from {geojson_file} into database...")
        imported_count = import_flood_geojson(geojson_file, db_config)
        print(f"Successfully imported {imported_count} features.")
    except Exception as e:
        print(f"Failed to import data: {e}")
        sys.exit(1)