# Pandas Patterns Used in This Portfolio

This document describes pandas patterns actively used across this repository
for analytics engineering workflows (clean → mart, DQ, reproducibility).

1. Grain validation
    grain = ["npi", "generic_name", "year"]
    assert df.duplicated(subset=grain).sum() == 0

2. Deterministic aggregations
    df_agg = (
        df.groupby(["npi", "year"], as_index=False)
          .agg(
              total_drug_cost=("total_drug_cost", "sum"),
              claim_count=("claim_id", "count")
          )
    )

3. Safe assignments
    df.loc[df["total_drug_cost"] < 0, "total_drug_cost"] = 0

4. Explicit year attribution
    df["year"] = 2023

5. Parquet write safety (avoid Arrow extension types)
    df = df.convert_dtypes(dtype_backend="numpy_nullable")
    df.to_parquet(path, engine="pyarrow", index=False)

6. Stable path handling
    from pathlib import Path
    PROJECT_ROOT = Path(__file__).resolve().parents[1]

7. Basic data quality checks
    assert df.shape[0] > 0
    assert df["total_drug_cost"].min() >= 0
