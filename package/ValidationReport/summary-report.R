
args <- commandArgs(trailingOnly = TRUE)
hive_db <- args[1]
target_db <- args[2]
campaign <- args[3]

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session()

# define function to pull data--------------------------------------------------
pull_data <- function (database) {
  scr_ext <- sql(paste0("
    SELECT

      FIRST(seg.gains_segment_code) AS gains_segment_code,
      FIRST(seg.gains_segment_description) AS gains_segment_description,
      COUNT(fct.marketing_efficiency) AS n_me,
      COUNT(fct.auto_quality_score) AS n_aqm,
      COUNT(fct.model_calculation_number) AS n_score,
      COUNT(fct.average_annual_renewal_premium) AS n_aarp,
      COUNT(fct.total_annual_renewal_premium) AS n_tarp,
      COUNT(fct.potential_profit) AS n_pp,
      COUNT(fct.present_value_future_profit) AS n_pvfp,
      COUNT(fct.conversion_score) AS n_conversion,
      COUNT(fct.aum_score) AS n_aum,
      COUNT(fct.retention_score) AS n_retention,

      AVG(fct.marketing_efficiency) AS avg_me,
      AVG(fct.auto_quality_score) AS avg_aqm,
      AVG(fct.model_calculation_number) AS avg_score,
      AVG(fct.average_annual_renewal_premium) AS avg_aarp,
      AVG(fct.total_annual_renewal_premium) AS avg_tarp,
      AVG(fct.potential_profit) AS avg_pp,
      AVG(fct.present_value_future_profit) AS avg_pvfp,
      AVG(fct.conversion_score) AS avg_conversion,
      AVG(fct.aum_score) AS avg_aum,
      AVG(fct.retention_score) AS avg_retention,

      PERCENTILE(CAST(fct.marketing_efficiency*100000000 AS BIGINT), 0.5) /
        100000000 AS median_me,
      PERCENTILE(CAST(fct.auto_quality_score*100000000 AS BIGINT), 0.5) /
        100000000 AS median_aqm,
      PERCENTILE(CAST(fct.model_calculation_number*100000000 AS BIGINT), 0.5) /
        100000000 AS median_score,
      PERCENTILE(CAST(fct.average_annual_renewal_premium*100000000 AS BIGINT),
                 0.5) / 100000000 AS median_aarp,
      PERCENTILE(CAST(fct.total_annual_renewal_premium*100000000 AS BIGINT),
                 0.5) / 100000000 AS median_tarp,
      PERCENTILE(CAST(fct.potential_profit*100000000 AS BIGINT), 0.5) /
        100000000 AS median_pp,
      PERCENTILE(CAST(fct.present_value_future_profit*100000000 AS BIGINT),
                 0.5) / 100000000 AS median_pvfp,
      PERCENTILE(CAST(fct.conversion_score*100000000 AS BIGINT), 0.5) /
        100000000 AS median_conversion,
      PERCENTILE(CAST(fct.aum_score*100000000 AS BIGINT), 0.5) /
        100000000 AS median_aum,
      PERCENTILE(CAST(fct.retention_score*100000000 AS BIGINT), 0.5) /
        100000000 AS median_retention

    FROM ", database, ".score_fact fct
    INNER JOIN ", database, ".score_segment seg ON
    fct.score_segment_skey = seg.score_segment_skey
    GROUP BY seg.gains_segment_code
    "))

  add_on <- sql(paste0("
    SELECT
      FIRST(seg.gains_segment_code) AS gains_segment_code,
      COUNT(ext.Eligage) AS n_age,
      AVG(ext.Eligage) AS avg_age,
      PERCENTILE(CAST(ext.Eligage AS BIGINT), 0.5) AS median_age,
      COUNT(ext.numtotall24mo) AS n_counter,
      AVG(ext.numtotall24mo) AS avg_counter,
      PERCENTILE(CAST(ext.numtotall24mo AS BIGINT), 0.5) AS median_counter,
      FIRST(ext.campaign_group) AS campaign_group
    FROM ", database, ".sc_scoring_extract ext
    INNER JOIN ", database, ".score_fact fct ON
      ext.Individual_Original_SKey = fct.Individual_Original_SKey AND
      ext.Organization_SKey        = fct.Organization_SKey
    INNER JOIN ", database, ".score_segment seg ON
      fct.score_segment_skey = seg.score_segment_skey
    GROUP BY seg.gains_segment_code
    "))

  createOrReplaceTempView(scr_ext, "scr_ext")
  createOrReplaceTempView(add_on, "add_on")

  sql("
    SELECT

      A.*,
      B.n_age,
      B.avg_age,
      B.median_age,
      B.n_counter,
      B.avg_counter,
      B.median_counter,
      B.campaign_group

    FROM scr_ext A
    INNER JOIN add_on B ON
      A.gains_segment_code = B.gains_segment_code
     ")
  }
  
new_table <- pull_data(hive_db)
createOrReplaceTempView(new_table, "new_table")

sql(paste0("INSERT INTO TABLE ", target_db, ".sf_summary
            SELECT * FROM new_table"))
