from data_loader import get_snowpark_session
import os

def run_sql_file(session, file_path):
    print(f"Executing {file_path}...")
    with open(file_path, 'r', encoding='utf-8') as f:
        sql = f.read()
    
    # Snowpark session.sql() might not handle multiple statements well if separated by ;
    # but these files usually have one main CREATE TABLE.
    # We should split by ; just in case.
    statements = sql.split(';')
    for stmt in statements:
        stmt = stmt.strip()
        if stmt:
            session.sql(stmt).collect()
    print(f"Successfully executed {file_path}")

if __name__ == 'main':
    try:
        session = get_snowpark_session()
        
        # Resolve absolute paths relative to this script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        sql_dir = os.path.join(os.path.dirname(script_dir), 'sql')
        
        # Run C2 (Update account-level features)
        run_sql_file(session, os.path.join(sql_dir, 'c2_account_features.sql'))
        
        # Run C3
        run_sql_file(session, os.path.join(sql_dir, 'c3_entity_features.sql'))
        
        # Run C7 (Entity Sentiment Features)
        run_sql_file(session, os.path.join(sql_dir, 'c7_sentiment_features.sql'))
        
        # Run C4 (regenerate master table with new features)
        run_sql_file(session, os.path.join(sql_dir, 'c4_target_table.sql'))

        # Run C5 (compound features and history flags)
        run_sql_file(session, os.path.join(sql_dir, 'c5_model_features.sql'))
        
        session.close()
        print("Pipeline updated successfully.")
    except Exception as e:
        print(f"ERROR: {str(e)}")
