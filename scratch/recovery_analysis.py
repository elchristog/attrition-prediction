import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from data_loader import get_snowpark_session
import snowflake.snowpark.functions as F
from snowflake.snowpark.window import Window

def run_recovery_analysis():
    """
    Analyzes the recovery rate of entities after a drop to zero volume.
    Using C4 table because it contains the raw ENT_GALLONS column.
    """
    session = get_snowpark_session()
    # Using ML_Attrition_Master_Table (C4) instead of C5 because C5 drops raw ENT_GALLONS
    source_table = "WORKSPACE.digitalda_stage.ML_Attrition_Master_Table"
    print(f"INFO: Analyzing recovery patterns from {source_table}...")
    
    # Load historical volume data
    df_vol = session.table(source_table).select("ORG_URI", "COHORT_MONTH", "ENT_GALLONS")
    
    # 1. Identify "Drop Events" (Month T)
    # Define window to detect the first month with zero volume after having activity
    window_spec = Window.partitionBy("ORG_URI").orderBy("COHORT_MONTH")
    # Snowpark uses snake_case for method names: with_column
    df_lags = df_vol.with_column("PREV_GALLONS", F.lag("ENT_GALLONS", 1).over(window_spec))
    
    # Filter for entities that dropped from >0 to 0 gallons
    df_drops = df_lags.filter((F.col("PREV_GALLONS") > 0) & (F.col("ENT_GALLONS") == 0))
    
    # 2. Track Recovery (Months T+1 to T+6)
    df_drop_cohorts = df_drops.select(
        F.col("ORG_URI").alias("DROP_ORG"),
        F.col("COHORT_MONTH").alias("DROP_MONTH")
    )
    
    # Join with future months for these specific entities
    df_future = df_vol.join(
        df_drop_cohorts, 
        (df_vol["ORG_URI"] == df_drop_cohorts["DROP_ORG"]) & 
        (df_vol["COHORT_MONTH"] > df_drop_cohorts["DROP_MONTH"])
    )
    
    # Calculate months elapsed and filter for a 6-month window
    df_future = df_future.with_column("MONTHS_SINCE_DROP", 
                                   F.datediff("month", F.col("DROP_MONTH"), F.col("COHORT_MONTH")))
    df_future = df_future.filter(F.col("MONTHS_SINCE_DROP") <= 6)
    
    # 3. Aggregate Recovery Statistics
    # Count unique entities that recovered (spent > 0) at each month N
    df_stats = df_future.groupBy("MONTHS_SINCE_DROP").agg(
        F.count_distinct(F.when(F.col("ENT_GALLONS") > 0, F.col("DROP_ORG"))).alias("RECOVERED_ENTITIES")
    ).to_pandas()
    
    total_drops = df_drop_cohorts.count()
    if total_drops == 0:
        print("ERROR: No drop events found. Check your data.")
        session.close()
        return

    df_stats['CUMULATIVE_RECOVERY_RATE'] = df_stats['RECOVERED_ENTITIES'] / total_drops
    df_stats = df_stats.sort_values("MONTHS_SINCE_DROP")
    
    # 4. Visualization for Business Justification
    plt.figure(figsize=(10, 6))
    sns.set_style("whitegrid")
    
    sns.lineplot(data=df_stats, x="MONTHS_SINCE_DROP", y="CUMULATIVE_RECOVERY_RATE", 
                        marker='o', color='#2c3e50')
    
    for x, y in zip(df_stats['MONTHS_SINCE_DROP'], df_stats['CUMULATIVE_RECOVERY_RATE']):
        plt.text(x, y + 0.01, f'{y:.1%}', ha='center', fontweight='bold')

    plt.title("Recovery Curve Post-Zero Volume Drop", fontsize=14)
    plt.xlabel("Months Elapsed Since Initial Drop", fontsize=12)
    plt.ylabel("Cumulative % of Recovered Entities", fontsize=12)
    
    plt.axvline(x=4, color='#e74c3c', linestyle='--', label="Proposed 4-Month Threshold")
    plt.legend()
    
    print("\n--- RECOVERY ANALYSIS RESULTS ---")
    print(f"Total drop events analyzed: {total_drops}")
    print(df_stats)
    
    plt.show()
    session.close()

if __name__ == "__main__":
    run_recovery_analysis()
