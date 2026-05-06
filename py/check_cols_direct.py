from data_loader import get_snowpark_session, load_training_data
import pandas as pd
import sys

try:
    session = get_snowpark_session()
    snowpark_df = load_training_data(session)
    df = snowpark_df.limit(1).to_pandas()
    print("COLUMNS_START")
    print(",".join(df.columns.tolist()))
    print("COLUMNS_END")
    session.close()
except Exception as e:
    print(f"ERROR: {str(e)}")
    sys.exit(1)
