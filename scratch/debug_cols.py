import sys
sys.path.append('py')
from data_loader import get_snowpark_session

session = get_snowpark_session()
try:
    print("Columns of Enterprise_Entity_Features_Rolling:")
    session.sql("DESCRIBE TABLE WORKSPACE.digitalda_stage.Enterprise_Entity_Features_Rolling").select("name", "type").show(100)
except Exception as e:
    print("Error:", e)
finally:
    session.close()
