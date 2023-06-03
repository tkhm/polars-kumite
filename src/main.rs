use polars::df;
use polars::prelude::*;
use polars_lazy::dsl::col;
use polars_lazy::prelude::*;

fn main() {
    let result = call();
    println!("ok? --> {}", result.is_ok());
}

fn call() -> anyhow::Result<()> {
    let df = df![
        "a" => [1, 2, 3],
        "b" => [None, Some("a"), Some("b")]
    ]?;

    // from an eager DataFrame
    let lf: LazyFrame = df.lazy();
    println!("{:?}", lf.collect()?);

    // scan a csv file lazily
    let lf: LazyFrame = LazyCsvReader::new("data/my.csv")
        .has_header(true)
        .finish()?;
    let filtered = lf.clone().filter(col("id").lt(lit(10))).collect()?;
    println!("{:?}", filtered);

    let sum = lf
        .filter(col("id").lt(lit(10)))
        .select(&[col("score")])
        .sum()
        .collect()?;
    println!("{:?}", sum.column("score")?.i64()?.get(0));

    // the following sample was not available
    // scan a parquet file lazily
    // let lf: LazyFrame = LazyFrame::scan_parquet("some_path", Default::default())?;
    Ok(())
}
