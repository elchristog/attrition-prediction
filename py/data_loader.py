import os
import pathlib

# Windows compatibility patch for Snowpark OSError [WinError 123]
# Intercepts invalid path checks during AST collection for modules like <frozen importlib>
_orig_is_file = pathlib.Path.is_file
def _patched_is_file(self):
    try:
        str_path = str(self)
        if "<" in str_path or ">" in str_path:
            return False
        return _orig_is_file(self)
    except Exception:
        return False
pathlib.Path.is_file = _patched_is_file

os.environ["SNOWPARK_SKIP_SOURCE_CODE_COLLECTION"] = "True"

import logging
from snowflake.snowpark import Session
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def get_snowpark_session():
    """
    Establishes a Snowpark session using credentials from environment variables.
    """
    connection_parameters = {
        "account": "bqa07840.us-west-2.privatelink",
        "user": os.getenv("SNOWFLAKE_USER"),
        "role": "DIGITALDA_ALL_ANALYST_ROLE",
        "warehouse": "DATA_SCIENCE_SP_LG_WH",
        "authenticator": "externalbrowser",
    }
    
    try:
        session = Session.builder.configs(connection_parameters).create()
        logger.info(f"Successfully connected to Snowflake as {connection_parameters['user']}")
        return session
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        raise

def run_pipeline_sql(session, sql_dir="model_pipeline/sql"):
    """
    Executes the C1 through C4 SQL pipeline in order.
    """
    pipeline_files = [
        "c1_target_variable.sql",
        "c2_account_features.sql",
        "c3_entity_features.sql",
        "c4_target_table.sql",
        "c5_model_features.sql"
    ]
    
    for filename in pipeline_files:
        file_path = os.path.join(sql_dir, filename)
        logger.info(f"Executing {filename}...")
        
        with open(file_path, "r", encoding="utf-8-sig") as f:
            sql_content = f.read().lstrip("\ufeff")
            
        # Snowpark session.sql() executes strings.
        # Note: If the SQL contains multiple statements (like CREATE OR REPLACE),
        # we might need to split them if the driver requires it, but usually CREATE OR REPLACE TABLE works fine.
        session.sql(sql_content).collect()
        logger.info(f"Finished {filename}")

def load_training_data(session, table_name="WORKSPACE.digitalda_stage.ML_Attrition_Master_Table"):
    """
    Loads the master table into a Snowpark DataFrame.
    """
    logger.info(f"Loading data from {table_name}...")
    return session.table(table_name)

if __name__ == "__main__":
    # Test connection and pipeline execution
    session = get_snowpark_session()
    # run_pipeline_sql(session) # Uncomment to run the full SQL waterfall
    df = load_training_data(session)
    print(f"Loaded {df.count()} rows from the master table.")
