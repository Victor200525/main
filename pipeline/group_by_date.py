import polars as pl
import config

def group_by_date():
    INPUT_DIR = config.STAGE_DIR
    # df = pl.read_parquet("output.parquet")
    df = pl.read_delta(INPUT_DIR)

    # Группируем и считаем средние
    result = (
        df.group_by("Date")
        .agg([
            pl.col("weight_balanced").mean().alias("avg_weight_balanced"),
        ])
        .sort("Date")
    )

    return result
