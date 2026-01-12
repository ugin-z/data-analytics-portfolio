def check_required_columns(df, required_cols):
    missing = required_cols - set(df.columns)
    assert not missing, f"Missing columns: {missing}"

def check_duplicates(df, key_cols):
    assert not df.duplicated(subset=key_cols).any(), "Duplicate rows detected"

def check_negative_revenue(df):
    assert (df["revenue"] >= 0).all(), "Negative revenue values found"