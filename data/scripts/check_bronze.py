import duckdb
c = duckdb.connect('c:/Users/yugsa/hcl_hackathon/data/bronze/retail_banking_bronze.duckdb')
tables = c.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main' AND table_name LIKE 'bronze_%' AND table_name NOT IN ('bronze_ingestion_log','bronze_ingestion_errors') ORDER BY table_name").fetchall()
for (tname,) in tables:
    print(f"\n=== {tname} ===")
    cols = c.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{tname}' ORDER BY ordinal_position").fetchall()
    for cname, ctype in cols:
        print(f"  {cname}: {ctype}")
    cnt = c.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()[0]
    print(f"  Row count: {cnt}")
c.close()