from data_loader import get_snowpark_session
import logging

logging.basicConfig(level=logging.INFO)

def inspect():
    session = get_snowpark_session()
    
    print("Checking 5 rows from Features...")
    session.sql("SELECT ORG_URI, COHORT_MONTH FROM WORKSPACE.digitalda_stage.Enterprise_Entity_Features_Rolling LIMIT 5").show()
    
    print("Checking 5 rows from Targets...")
    session.sql("SELECT ORG_URI, EVALUATION_MONTH FROM WORKSPACE.digitalda_stage.entity_Christian_target_variable LIMIT 5").show()
    
    print("Checking if any ORG_URIs match...")
    sql_match = """
    SELECT COUNT(*) 
    FROM WORKSPACE.digitalda_stage.Enterprise_Entity_Features_Rolling f
    JOIN WORKSPACE.digitalda_stage.entity_Christian_target_variable t
    ON f.ORG_URI = t.ORG_URI
    """
    session.sql(sql_match).show()

    print("Checking if any ORG_URI + MONTH match...")
    sql_match_all = """
    SELECT COUNT(*) 
    FROM WORKSPACE.digitalda_stage.Enterprise_Entity_Features_Rolling f
    JOIN WORKSPACE.digitalda_stage.entity_Christian_target_variable t
    ON f.ORG_URI = t.ORG_URI AND f.COHORT_MONTH = t.EVALUATION_MONTH
    """
    session.sql(sql_match_all).show()

if __name__ == "__main__":
    inspect()
