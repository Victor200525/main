import polars as pl

def group_by_date():
    # Читаем parquet
    df = pl.read_parquet("output.parquet")

    # Группируем и считаем средние
    result = (
        df.group_by("Date")
        .agg([
            pl.col("weight_balanced").mean().alias("avg_weight_balanced"),
        ])
        .sort("Date")
    )

    # Сохраняем в новый parquet
    result.write_parquet("aggregated_weights.parquet")

    print("Данные сохранены в aggregated_weights.parquet")
