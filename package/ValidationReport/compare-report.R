
args <- commandArgs(trailingOnly = TRUE)
target_db <- args[1]
new <- args[2]
old <- args[3]
campaign <- args[4]

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session()

# current campaign information--------------------------------------------------
new_table <- sql(paste0("SELECT * FROM ", new, ""))
createOrReplaceTempView(new_table, "new_table")

# previous campaign information-------------------------------------------------
old_table <- sql(paste0("SELECT * FROM ", old, ""))
createOrReplaceTempView(old_table, "old_table")

#Combine two tables and create report-------------------------------------------
vr <- sql("
SELECT

  new.gains_segment_code,
  new.gains_segment_description,
  new.n_me AS new_n_me,
  old.n_me AS old_n_me,
  (new.n_me - old.n_me) / old.n_me AS pc_n_me,
  new.avg_me AS new_avg_me,
  old.avg_me AS old_avg_me,
  (new.avg_me - old.avg_me) / old.avg_me AS pc_avg_me,
  new.median_me AS new_median_me,
  old.median_me AS old_median_me,
  (new.median_me - old.median_me) / old.median_me AS pc_median_me,
  new.n_aqm AS new_n_aqm,
  old.n_aqm AS old_n_aqm,
  (new.n_aqm - old.n_aqm) / old.n_aqm AS pc_n_aqm,
  new.avg_aqm AS new_avg_aqm,
  old.avg_aqm AS old_avg_aqm,
  (new.avg_aqm - old.avg_aqm) / old.avg_aqm AS pc_avg_aqm,
  new.median_aqm AS new_median_aqm,
  old.median_aqm AS old_median_aqm,
  (new.median_aqm - old.median_aqm) / old.median_aqm AS pc_median_aqm,
  new.n_score AS new_n_score,
  old.n_score AS old_n_score,
  (new.n_score - old.n_score) / old.n_score AS pc_n_score,
  new.avg_score AS new_avg_score,
  old.avg_score AS old_avg_score,
  (new.avg_score- old.avg_score) / old.avg_score AS pc_avg_score,
  new.median_score AS new_median_score,
  old.median_score AS old_median_score,
  (new.median_score- old.median_score) / old.median_score AS pc_median_score,
  new.n_aarp AS new_n_aarp,
  old.n_aarp AS old_n_aarp,
  (new.n_aarp - old.n_aarp) / old.n_aarp AS pc_n_aarp,
  new.avg_aarp AS new_avg_aarp,
  old.avg_aarp AS old_avg_aarp,
  (new.avg_aarp- old.avg_aarp) / old.avg_aarp AS pc_avg_aarp,
  new.median_aarp AS new_median_aarp,
  old.median_aarp AS old_median_aarp,
  (new.median_aarp- old.median_aarp) / old.median_aarp AS pc_median_aarp,
  new.n_tarp AS new_n_tarp,
  old.n_tarp AS old_n_tarp,
  (new.n_tarp - old.n_tarp) / old.n_tarp AS pc_n_tarp,
  new.avg_tarp AS new_avg_tarp,
  old.avg_tarp AS old_avg_tarp,
  (new.avg_tarp- old.avg_tarp) / old.avg_tarp AS pc_avg_tarp,
  new.median_tarp AS new_median_tarp,
  old.median_tarp AS old_median_tarp,
  (new.median_tarp- old.median_tarp) / old.median_tarp AS pc_median_tarp,
  new.n_pp AS new_n_pp,
  old.n_pp AS old_n_pp,
  (new.n_pp - old.n_pp) / old.n_aqm AS pc_n_pp,
  new.avg_pp AS new_avg_pp,
  old.avg_pp AS old_avg_pp,
  (new.avg_pp- old.avg_pp) / old.avg_pp AS pc_avg_pp,
  new.median_pp AS new_median_pp,
  old.median_pp AS old_median_pp,
  (new.median_pp- old.median_pp) / old.median_pp AS pc_median_pp,
  new.n_pvfp AS new_n_pvfp,
  old.n_pvfp AS old_n_pvfp,
  (new.n_pvfp - old.n_pvfp) / old.n_pvfp AS pc_n_pvfp,
  new.avg_pvfp AS new_avg_pvfp,
  old.avg_pvfp AS old_avg_pvfp,
  (new.avg_pvfp- old.avg_pvfp) / old.avg_pvfp AS pc_avg_pvfp,
  new.median_pvfp AS new_median_pvfp,
  old.median_pvfp AS old_median_pvfp,
  (new.median_pvfp- old.median_pvfp) / old.median_pvfp AS pc_median_pvfp,
  new.n_conversion AS new_n_conversion,
  old.n_conversion AS old_n_conversion,
  (new.n_conversion - old.n_conversion) / old.n_conversion AS pc_n_conversion,
  new.avg_conversion AS new_avg_conversion,
  old.avg_conversion AS old_avg_conversion,
  (new.avg_conversion- old.avg_conversion) /
    old.avg_conversion AS pc_avg_conversion,
  new.median_conversion AS new_median_conversion,
  old.median_conversion AS old_median_conversion,
  (new.median_conversion- old.median_conversion) /
    old.median_conversion AS pc_median_conversion,
  new.n_aum AS new_n_aum,
  old.n_aum AS old_n_aum,
  (new.n_aum - old.n_aum) / old.n_aum AS pc_n_aum,
  new.avg_aum AS new_avg_aum,
  old.avg_aum AS old_avg_aum,
  (new.avg_aum- old.avg_aum) / old.avg_aum AS pc_avg_aum,
  new.median_aum AS new_median_aum,
  old.median_aum AS old_median_aum,
  (new.median_aum- old.median_aum) / old.median_aum AS pc_median_aum,
  new.n_retention AS new_n_retention,
  old.n_retention AS old_n_retention,
  (new.n_retention - old.n_retention) / old.n_retention AS pc_n_retention,
  new.avg_retention AS new_avg_retention,
  old.avg_retention AS old_avg_retention,
  (new.avg_retention- old.avg_retention) /
    old.avg_retention AS pc_avg_retention,
  new.median_retention AS new_median_retention,
  old.median_retention AS old_median_retention,
  (new.median_retention- old.median_retention) /
    old.median_retention AS pc_median_retention,
  new.n_age AS new_n_age,
  old.n_age AS old_n_age,
  (new.n_age - old.n_age) / old.n_age AS pc_n_age,
  new.avg_age AS new_avg_age,
  old.avg_age AS old_avg_age,
  (new.avg_age- old.avg_age) / old.avg_age AS pc_avg_age,
  new.median_age AS new_median_age,
  old.median_age AS old_median_age,
  (new.median_age- old.median_age) / old.median_age AS pc_median_age,
  new.n_counter AS new_n_counter,
  old.n_counter AS old_n_counter,
  (new.n_counter - old.n_counter) / old.n_counter AS pc_n_counter,
  new.avg_counter AS new_avg_counter,
  old.avg_counter AS old_avg_counter,
  (new.avg_counter- old.avg_counter) / old.avg_counter AS pc_avg_counter,
  new.median_counter AS new_median_counter,
  old.median_counter AS old_median_counter,
  (new.median_counter- old.median_counter) /
    old.median_counter AS pc_median_counter

FROM new_table new
INNER JOIN old_table old ON
new.gains_segment_code = old.gains_segment_code
")

persist(vr, "MEMORY_ONLY")
createOrReplaceTempView(vr, "vr")

vr2 <- sql("
SELECT *
FROM (
  SELECT
    gains_segment_code,
    gains_segment_description,
    'ME' AS variable,
    new_n_me AS Count,
    old_n_me AS Count_old,
    pc_n_me AS Count_Pct_Chg,
    new_avg_me AS Avg,
    old_avg_me AS Avg_Old,
    pc_avg_me AS Avg_Pct_Chg,
    new_median_me AS Median,
    old_median_me AS Median_Old,
    pc_median_me AS Median_Pct_Chg

  FROM vr
  UNION ALL
  SELECT
    gains_segment_code,
    gains_segment_description,
    'AQM' AS variable,
    new_n_aqm AS new_n,
    old_n_aqm AS Count_old,
    pc_n_aqm AS Count_Pct_Chg,
    new_avg_aqm AS Avg,
    old_avg_aqm AS Avg_Old,
    pc_avg_aqm AS Avg_Pct_Chg,
    new_median_aqm AS Median,
    old_median_aqm AS Median_Old,
    pc_median_aqm AS Median_Pct_Chg

  FROM vr
  UNION ALL
  SELECT
    gains_segment_code,
    gains_segment_description,
    'Score' AS variable,
    new_n_score AS Count,
    old_n_score AS Count_old,
    pc_n_score AS Count_Pct_Chg,
    new_avg_score AS Avg,
    old_avg_score AS Avg_Old,
    pc_avg_score AS Avg_Pct_Chg,
    new_median_score AS Median,
    old_median_score AS Median_Old,
    pc_median_score AS Median_Pct_Chg

  FROM vr
  UNION ALL
  SELECT
    gains_segment_code,
    gains_segment_description,
    'AARP' AS variable,
    new_n_aarp AS Count,
    old_n_aarp AS Count_old,
    pc_n_aarp AS Count_Pct_Chg,
    new_avg_aarp AS Avg,
    old_avg_aarp AS Avg_Old,
    pc_avg_aarp AS Avg_Pct_Chg,
    new_median_aarp AS Median,
    old_median_aarp AS Median_Old,
    pc_median_aarp AS Median_Pct_Chg

  FROM vr
  UNION ALL
  SELECT
    gains_segment_code,
    gains_segment_description,
    'TARP' AS variable,
    new_n_tarp AS Count,
    old_n_tarp AS Count_old,
    pc_n_tarp AS Count_Pct_Chg,
    new_avg_tarp AS Avg,
    old_avg_tarp AS Avg_Old,
    pc_avg_tarp AS Avg_Pct_Chg,
    new_median_tarp AS Median,
    old_median_tarp AS Median_Old,
    pc_median_tarp AS Median_Pct_Chg

  FROM vr
  UNION ALL
  SELECT
    gains_segment_code,
    gains_segment_description,
    'PP' AS variable,
    new_n_pp AS Count,
    old_n_pp AS Count_old,
    pc_n_pp AS Count_Pct_Chg,
    new_avg_pp AS Avg,
    old_avg_pp AS Avg_Old,
    pc_avg_pp AS Avg_Pct_Chg,
    new_median_pp AS Median,
    old_median_pp AS Median_Old,
    pc_median_pp AS Median_Pct_Chg

  FROM vr
  UNION ALL
  SELECT
    gains_segment_code,
    gains_segment_description,
    'PVFP' AS variable,
    new_n_pvfp AS Count,
    old_n_pvfp AS Count_old,
    pc_n_pvfp AS Count_Pct_Chg,
    new_avg_pvfp AS Avg,
    old_avg_pvfp AS Avg_Old,
    pc_avg_pvfp AS Avg_Pct_Chg,
    new_median_pvfp AS Median,
    old_median_pvfp AS Median_Old,
    pc_median_pvfp AS Median_Pct_Chg

  FROM vr
  UNION ALL
  SELECT
    gains_segment_code,
    gains_segment_description,
    'Age' AS variable,
    new_n_age AS Count,
    old_n_age AS Count_old,
    pc_n_age AS Count_Pct_Chg,
    new_avg_age AS Avg,
    old_avg_age AS Avg_Old,
    pc_avg_age AS Avg_Pct_Chg,
    new_median_age AS Median,
    old_median_age AS Median_Old,
    pc_median_age AS Median_Pct_Chg

  FROM vr
  UNION ALL
  SELECT
    gains_segment_code,
    gains_segment_description,
    'COUNTER' AS variable,
    new_n_counter AS Count,
    old_n_counter AS Count_old,
    pc_n_counter AS Count_Pct_Chg,
    new_avg_counter AS Avg,
    old_avg_counter AS Avg_Old,
    pc_avg_counter AS Avg_Pct_Chg,
    new_median_counter AS Median,
    old_median_counter AS Median_Old,
    pc_median_counter AS Median_Pct_Chg

  FROM vr
  UNION ALL
  SELECT
    gains_segment_code,
    gains_segment_description,
    'Conversion' AS variable,
    new_n_conversion AS Count,
    old_n_conversion AS Count_old,
    pc_n_conversion AS Count_Pct_Chg,
    new_avg_conversion AS Avg,
    old_avg_conversion AS Avg_Old,
    pc_avg_conversion AS Avg_Pct_Chg,
    new_median_conversion AS Median,
    old_median_conversion AS Median_Old,
    pc_median_conversion AS Median_Pct_Chg

  FROM vr
  UNION ALL
  SELECT
    gains_segment_code,
    gains_segment_description,
    'AUM' AS variable,
    new_n_aum AS Count,
    old_n_aum AS Count_old,
    pc_n_aum AS Count_Pct_Chg,
    new_avg_aum AS Avg,
    old_avg_aum AS Avg_Old,
    pc_avg_aum AS Avg_Pct_Chg,
    new_median_aum AS Median,
    old_median_aum AS Median_Old,
    pc_median_aum AS Median_Pct_Chg

  FROM vr
  UNION ALL
  SELECT
    gains_segment_code,
    gains_segment_description,
    'Retention' AS variable,
    new_n_retention AS Count,
    old_n_retention AS Count_old,
    pc_n_retention AS Count_Pct_Chg,
    new_avg_retention AS Avg,
    old_avg_retention AS Avg_Old,
    pc_avg_retention AS Avg_Pct_Chg,
    new_median_retention AS Median,
    old_median_retention AS Median_Old,
    pc_median_retention AS Median_Pct_Chg

  FROM vr) AS CombinedTable
")

dropTempView("vr")
unpersist(vr)
persist(vr2, "MEMORY_ONLY")
createOrReplaceTempView(vr2, "vr2")

validation_report <- sql("
SELECT

  *,
  CASE
    WHEN Count > 100000 THEN 10
    ELSE 0
  END AS Size_Flag,
  CASE
    WHEN Count_Pct_Chg > 0.02 THEN 1
    ELSE 0
  END AS Count_Pct_Chg_Flag,
  CASE
    WHEN Avg_Pct_Chg > 0.02 THEN 1
    ELSE 0
  END AS Avg_Pct_Chg_Flag,
  CASE
    WHEN Median_Pct_Chg > 0.02 THEN 1
    ELSE 0
  END AS Median_Pct_Chg_Flag

FROM vr2
")

dropTempView("vr2")
unpersist(vr2)
persist(validation_report, "MEMORY_ONLY")
validation_report$Total_Flag <- validation_report$Size_Flag +
                                validation_report$Count_Pct_Chg_Flag +
                                validation_report$Avg_Pct_Chg_Flag +
                                validation_report$Median_Pct_Chg_Flag
createOrReplaceTempView(validation_report, "validation_report")

sql(paste0("CREATE TABLE IF NOT EXISTS ", target_db, ".validation_report_",
           campaign, " AS SELECT * FROM validation_report"))
