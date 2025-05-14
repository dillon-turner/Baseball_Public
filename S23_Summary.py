import duckdb
import numpy as np
import pandas as pd
import plotly.express as px

# Create a database file on disk
conn = duckdb.connect('example.db')
# Enable remote access
conn.sql("INSTALL httpfs")
conn.sql("LOAD httpfs")
# This database file points to files totaling multiple GBs,
# but it's only about 300KB itself. The `ATTACH` command
# gives us access to views that sit on top of remote Parquet files.
try:
  conn.sql("ATTACH 'https://data.baseball.computer/dbt/bc_remote.db' (READ_ONLY)")
except duckdb.BinderException:
  # This command will fail if you run it more than once because it already exists,
  # in which case we don't need to do anything
  pass

conn.sql("USE bc_remote")
conn.sql("USE main_models")

df: pd.DataFrame = conn.sql("""WITH players AS 
                            (SELECT DISTINCT
                                r.player_id, 
                                r.last_name, 
                                r.first_name 
                            FROM misc.roster r  
                            WHERE r.year in (2019,2020,2021,2022,2023) 
                            ), 
                            offStats AS ( 
                            SELECT DISTINCT
                                gl.season,
                                p.last_name, 
                                p.first_name, 
                                sum(ebs.hits) AS hits,
                                sum(ebs.home_runs) AS HRs,
                                sum(ebs.plate_appearances) AS PAs
                            FROM event_batting_stats ebs 
                            left join players p on ebs.batter_id = p.player_id 
                            left join stg_gamelog gl on ebs.game_id = gl.game_id
                            where gl.season IN (2019,2020,2021,2022,2023) 
                            GROUP BY gl.season, p.player_id, p.last_name, p.first_name
                            )
                            SELECT * FROM offStats;""").df()
    
printdf = df.sort_values('hits', ascending = False).head(n = 25)

print(printdf)

fig = px.scatter(df, x = 'HRs', y = 'hits', color ="PAs", facet_col = "season")

fig.show()