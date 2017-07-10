library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
library(TS.utilities)
library(ADD.scoring)

sparkR.session()
args <- commandArgs(trailingOnly = TRUE)
hive_db <- args[1]
target_db <- args[2]
campaign_group <- args[3]

ac_prod_num <- get_ac_product_skey(hive_db, campaign_group, "AD3N")

sql_string <- create_sql_pull_string_add_NonContributory(
  paste0(hive_db, ".sc_scoring_extract"), campaign_group, ac_prod_num)
new_data <- sql(sql_string)

# Re-partition
new_data <- repartition(new_data, numPartitions = 1000)

colnames(new_data) <- tolower(colnames(new_data))
scores_df <- new_data[, c("individual_original_skey", "organization_skey",
                          "aarp_prod_cd", "ppf_prod_cd", "gendermfu_extract", 
                          "age_cat", "automated_payment_flag",
                          "billtype", "organizationppfsegment", "income_range",
                          "contract", "ac_product_skey")]
scores_df$prob_score <- lit(0.0000)
scores_df$predicted_nap <- lit(0.0000)
# convert organization_skey into numeric to fix dapply type matching problem
temp_type <- coltypes(scores_df)
temp_type[2] <- 'numeric'
coltypes(scores_df) <- temp_type
out_schema <- schema(scores_df)

sub_scr_ext <- dapply(new_data,
                      function(r_df){
                        .libPaths("./myRLibs")
                        ADD.scoring::predict_add_NonContributory_fn(r_df)
                      },
                      out_schema)

createOrReplaceTempView(sub_scr_ext, "sub_scr_ext")

model_ppf <- get_ppf_DF(hive_db, 'ADD', '8/5/2016')
createOrReplaceTempView(model_ppf, "ppf_current")

# NOTE: From TS.utility, sa_cost_pm does not multiply by 0.001.
# SA_Cost_pm from table score_segment are all 0 for ADD this time.
# Thus, not multyply by 0.001 does not matter.

mkt_cost_N <- get_mktcost(hive_db, "431", "ADD", campaign_group)
# Change name to Navy
temp_colnames <- colnames(mkt_cost_N)
temp_colnames[which(temp_colnames %in% 'sa_cost_pm')] <- 'SA_Cost_pm_N'
temp_colnames[which(temp_colnames %in% 'mktcost')] <- 'mkt_cost_navy'
colnames(mkt_cost_N) <- temp_colnames
createOrReplaceTempView(mkt_cost_N, "mkt_cost_N")

mkt_cost_NN <- get_mktcost(hive_db, "429", "ADD", campaign_group)
# Change name to Non-Navy
temp_colnames <- colnames(mkt_cost_NN)
temp_colnames[which(temp_colnames %in% 'sa_cost_pm')] <- 'SA_Cost_pm_NN'
temp_colnames[which(temp_colnames %in% 'mktcost')] <- 'mkt_cost_nonnavy'
colnames(mkt_cost_NN) <- temp_colnames
createOrReplaceTempView(mkt_cost_NN, "mkt_cost_NN")

model_score_all <- sql("
SELECT
  0 AS Score_SKey,
  SC.Individual_Original_SKey,
  SC.Organization_SKey,
  predicted_nap AS AARP_amount,
  CASE
    WHEN SC.contract = 04500218 THEN MN. Score_Segment_SKey
    WHEN SC.contract NOT IN (04500218) THEN MNN. Score_Segment_SKey
  END AS Score_Segment_SKey,
  SC.AC_Product_Skey,
  prob_score AS Model_Calculation_Number,
  prob_score AS Adjusted_Model_Score,
  CASE
    WHEN SC.contract = 04500218 THEN predicted_nap
    WHEN SC.contract NOT IN (04500218) THEN predicted_nap 
  END AS Average_Annual_Renewal_Premium,
  CASE
  WHEN SC.contract = 04500218 THEN MN.mkt_cost_navy + MN.SA_Cost_pm_N +
    (prob_score * MN.Variable_Cost_pb)
  WHEN SC.contract NOT IN (04500218) THEN MNN.mkt_cost_nonnavy +
  MNN.SA_Cost_pm_NN + (prob_score * MNN.Variable_Cost_pb)
  END AS Marketing_Cost,
  CASE
    WHEN SC.contract = 04500218 THEN prob_score * (predicted_nap )
    WHEN SC.contract NOT IN (04500218) THEN  prob_score * (predicted_nap )
  END AS Total_Annual_Renewal_Premium,
  CASE
    WHEN SC.contract = 04500218 THEN (prob_score * (predicted_nap)) *
      PP.ppf_amount
    WHEN SC.contract NOT IN (04500218) THEN (prob_score * predicted_nap) *
      PP.ppf_amount
  END AS Potential_Profit,
  CASE
    WHEN SC.contract = 04500218 THEN ((prob_score * predicted_nap) *
      PP.ppf_amount) - (MN.mkt_cost_navy + MN.SA_Cost_pm_N +
      (prob_score * MN.Variable_Cost_pb))
    WHEN SC.contract NOT IN (04500218) THEN ((prob_score * predicted_nap) *
      PP.ppf_amount) - (MNN.mkt_cost_nonnavy + MNN.SA_Cost_pm_NN +
      (prob_score * MNN.Variable_Cost_pb))
  END AS Present_Value_Future_Profit

FROM  sub_scr_ext AS SC
INNER JOIN ppf_current AS PP ON
  SC.PPF_Prod_CD            = PP.product_code_lookup_ppf AND
  SC.gendermfu_extract      = PP.gendermfu_lookup_ppf AND
  SC.Automated_Payment_Flag = PP.Automated_Payment_Flag AND
  SC.billtype               = PP.PPF_General AND
  SC.OrganizationPPFSegment = PP.organizationppfsegment
INNER join mkt_cost_N MN ON
  SC.PPF_Prod_CD = MN.prod_code
INNER join mkt_cost_NN MNN ON
  SC.PPF_Prod_CD = MNN.prod_code
")

createOrReplaceTempView(model_score_all, "model_score_all")

dropTempView("sub_scr_ext")
dropTempView("ppf_current")
dropTempView("mkt_cost_NN")
dropTempView("mkt_cost_N")

result <- get_ranks_via_case("model_score_all", "Present_Value_Future_Profit",
                              0.05, "dsc")
createOrReplaceTempView(result, "result")

result <- sql(paste0("
SELECT

  *,
  Model_Calculation_Number AS Model_Score,
  Present_Value_Future_Profit / (Present_Value_Future_Profit +
  Marketing_Cost) AS Marketing_Efficiency
  
  FROM result
"))

createOrReplaceTempView(result, "result")

score_model_skey <- get_score_model_skey(hive_db, campaign_group, 135)

sql(paste0("
INSERT INTO TABLE ", target_db, ".score_fact
SELECT

  0 AS Score_SKey,
  Individual_Original_SKey,
  Organization_SKey,",
  score_model_skey, " AS Score_Model_SKey,
  Score_Segment_SKey,
  ac_product_skey,
  Model_Calculation_number, 
  Model_Score,
  Adjusted_Model_Score,
  Total_Annual_Renewal_Premium,
  Average_Annual_Renewal_Premium,
  Present_Value_Future_Profit,
  Potential_Profit,
  Marketing_Cost,
  Marketing_Efficiency,
  ranks as Pentile_number,
  NULL AS Auto_Quality_Score,
  NULL AS Student_Loan_Prospect_SKey,
  NULL AS Conversion_Score,
  NULL AS AUM_Score,
  NULL AS Retention_Score,
  NULL AS Retention_Pentile

  FROM result
"))
