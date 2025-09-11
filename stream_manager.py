from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import os

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'azuredata'),
        user=os.getenv('DB_USER', 'azureuser'),
        password=os.getenv('DB_PASSWORD', 'azurepass'),
        port=os.getenv('DB_PORT', '5432')
    )
    return conn

@app.route('/app_memory_daily', methods=['POST'])
def handle_app_memory_daily():
    data = request.get_json()
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Insert owner if not exists
        cur.execute(
            "INSERT INTO owners (hash_owner) VALUES (%s) ON CONFLICT DO NOTHING",
            (data['HashOwner'],)
        )
        
        # Insert app if not exists
        cur.execute(
            """INSERT INTO apps (hash_app, hash_owner) 
            VALUES (%s, %s) ON CONFLICT DO NOTHING""",
            (data['HashApp'], data['HashOwner'])
        )
        
        # Insert memory data
        cur.execute(
            """INSERT INTO fact_app_memory_daily (
                hash_app, day, sample_count, avg_mb, p1, p5, p25, 
                p50, p75, p95, p99, p100
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )""",
            (
                data['HashApp'], data['Day'], data['SampleCount'],
                data['AverageAllocatedMb'], data['AverageAllocatedMb_pct1'],
                data['AverageAllocatedMb_pct5'], data['AverageAllocatedMb_pct25'],
                data['AverageAllocatedMb_pct50'], data['AverageAllocatedMb_pct75'],
                data['AverageAllocatedMb_pct95'], data['AverageAllocatedMb_pct99'],
                data['AverageAllocatedMb_pct100']
            )
        )
        
        conn.commit()
        return jsonify({"status": "success"}), 201
        
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/function_duration_daily', methods=['POST'])
def handle_function_duration_daily():
    data = request.get_json()
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get or create function_id
        cur.execute(
            "SELECT function_id FROM function_mapping WHERE hash_function = %s",
            (data['HashFunction'],)
        )
        function_id = cur.fetchone()
        
        if not function_id:
            cur.execute(
                "INSERT INTO function_mapping (hash_function) VALUES (%s) RETURNING function_id",
                (data['HashFunction'],)
            )
            function_id = cur.fetchone()[0]
            
            # Insert into functions table
            cur.execute(
                "INSERT INTO functions (function_id, hash_app, trigger) VALUES (%s, %s, %s)",
                (function_id, data['HashApp'], data.get('Trigger', 'default'))
            )
        else:
            function_id = function_id[0]
        
        # Insert duration data
        cur.execute(
            """INSERT INTO fact_function_duration_daily (
                function_id, day, avg_ms, min_ms, max_ms, p0, p1, p25, p50,
                p75, p99, p100, count
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )""",
            (
                function_id, data['Day'], data['Average'], data['Minimum'],
                data['Maximum'], data['percentile_Average_0'],
                data['percentile_Average_1'], data['percentile_Average_25'],
                data['percentile_Average_50'], data['percentile_Average_75'],
                data['percentile_Average_99'], data['percentile_Average_100'],
                data['Count']
            )
        )
        
        conn.commit()
        return jsonify({"status": "success"}), 201
        
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/invocations_minutely', methods=['POST'])
def handle_invocations_minutely():
    data = request.get_json()
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get or create function_id
        cur.execute(
            "SELECT function_id FROM function_mapping WHERE hash_function = %s",
            (data['HashFunction'],)
        )
        function_id = cur.fetchone()
        
        if not function_id:
            cur.execute(
                "INSERT INTO function_mapping (hash_function) VALUES (%s) RETURNING function_id",
                (data['HashFunction'],)
            )
            function_id = cur.fetchone()[0]
            
            # Insert into functions table
            cur.execute(
                "INSERT INTO functions (function_id, hash_app, trigger) VALUES (%s, %s, %s)",
                (function_id, data['HashApp'], data['Trigger'])
            )
        else:
            function_id = function_id[0]
        
        # Prepare usage array
        usage_array = []
        for minute in range(1, 1441):
            usage_array.append(data[str(minute)])
        
        # Insert into minutely table
        cur.execute(
            "INSERT INTO fact_invocations_minutely_sparse (function_id, day, usage) VALUES (%s, %s, %s)",
            (function_id, data['Day'], usage_array)
        )
        
        conn.commit()
        return jsonify({"status": "success"}), 201
        
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    app.run(debug=True)