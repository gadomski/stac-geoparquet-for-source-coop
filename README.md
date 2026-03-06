# stac-geoparquet for source.coop

Small Rust executable that:

1. Finds all [Microsoft Planetary Computer](https://planetarycomputer.microsoft.com/) **stac-geoparquet** files under a given prefix for a given year
2. Reads all items and prepends a [spatio-temporal hash](https://github.com/radiantearth/stac-spec/discussions/1378) to their ids
3. Writes the **stac-geoparquet** files back down to disc

After writing, we then combine all the **stac-geoparquet** files into a single file, sorted by id (and therefore by space and time):

```sql
copy (select * from read_parquet('*.parquet') order by id) to 'outfile.parquet' (format parquet, compression zstd);
```

For big datasets like Sentinel 2 L2A, this can require a computer with lots of memory; for our initial experiments, we used an AWS EC2 `r8g.4xlarge`.
