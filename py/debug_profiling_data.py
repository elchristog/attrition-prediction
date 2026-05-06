from data_loader import get_snowpark_session, load_training_data
import pandas as pd

def debug_data():
    session = get_snowpark_session()
    snowpark_df = load_training_data(session)
    df = snowpark_df.sample(0.001).to_pandas() # Smaller sample for speed
    df.columns = [c.upper() for c in df.columns]
    
    print("\nColumns and Types:")
    print(df.dtypes)
    
    print("\nCohort Month Range:")
    print(df['COHORT_MONTH'].min(), "to", df['COHORT_MONTH'].max())
    
    print("\nExclusion Flag Distributions:")
    print(df[['EXCL_FLAG_3_6M', 'EXCL_FLAG_3_8M', 'EXCL_FLAG_3_12M']].value_counts())
    
    print("\nTarget Distributions:")
    print(df[['TARGET_6M', 'TARGET_8M', 'TARGET_12M']].value_counts())
    
    # Check if anything would pass the filters
    test_cutoff = df['COHORT_MONTH'].max() - pd.DateOffset(months=3)
    train_slice = df[df['COHORT_MONTH'] <= test_cutoff]
    print(f"\nTrain Slice size: {len(train_slice)}")
    
    if len(train_slice) > 0:
        filtered = train_slice[train_slice['EXCL_FLAG_3_6M'] == 0]
        print(f"Filtered Train (6M) size: {len(filtered)}")
        if len(filtered) > 0:
            print(f"Positive cases in filtered train: {filtered['TARGET_6M'].sum()}")

if __name__ == "__main__":
    debug_data()
