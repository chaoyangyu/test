library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
library(TS.utilities)
library(ADD.scoring)
library(xgboost)

sparkR.session()

args <- commandArgs(trailingOnly = TRUE)
hive_db <- args[1]
target_db <- args[2]
campaign_group <- args[3]

# hive_db <- 'add_model'
# target_db <- 'scoring_processing_201706'
# campaign_group <- 201706

ac_prod_cust_num <- get_ac_product_skey(hive_db, campaign_group, "AD3C")
ac_prod_pros_num <- get_ac_product_skey(hive_db, campaign_group, "AD3P")


sql_pull_str <- create_clean_sql_pull_string(paste0(hive_db, ".sc_scoring_extract"),
                                       campaign_group,
                                       ac_prod_cust_num, ac_prod_pros_num)

scr_ext <- sql(sql_pull_str)

scr_ext <- repartition(scr_ext, numPartitions = 1000)
createOrReplaceTempView(scr_ext, "scr_ext")

scores_df <- scr_ext[, c("individual_original_skey", "organization_skey",
                         "aarp_prod_cd", "ppf_prod_cd", "gendermfu_extract", 
                         "age_cat", "automated_payment_flag",
                         "billtype", "organizationppfsegment", "income_range",
                         "contract", "ac_product_skey")]
scores_df$prob_score <- lit(0.0000)
scores_df$aarp <- lit(0.0000)
out_schema <- schema(scores_df)
scr_ext <- dapply(scr_ext,
                 function(r_df){
                   .libPaths("./myRLibs")
                   ADD.scoring::predict_add_pros_fn(r_df)
                  },
                 out_schema)
persist(scr_ext, "MEMORY_ONLY")
createOrReplaceTempView(scr_ext, "scr_ext")

model_ppf <- get_ppf_DF(hive_db, 'ADD', '8/5/2016')
createOrReplaceTempView(model_ppf, "model_ppf")

mkt_cost_N <- get_mktcost(hive_db, "430", 'ADD', campaign_group)
# Change name to Navy
temp_colnames <- colnames(mkt_cost_N)
temp_colnames[which(temp_colnames %in% 'sa_cost_pm')] <- 'SA_Cost_pm_N'
temp_colnames[which(temp_colnames %in% 'mktcost')] <- 'mkt_cost_navy'
colnames(mkt_cost_N) <- temp_colnames

mkt_cost_NN <- get_mktcost(hive_db, "428", 'ADD', campaign_group)
# Change name to Non-Navy
temp_colnames <- colnames(mkt_cost_NN)
temp_colnames[which(temp_colnames %in% 'sa_cost_pm')] <- 'SA_Cost_pm_NN'
temp_colnames[which(temp_colnames %in% 'mktcost')] <- 'mkt_cost_nonnavy'
colnames(mkt_cost_NN) <- temp_colnames

createOrReplaceTempView(model_ppf, "ppf_current")
createOrReplaceTempView(mkt_cost_NN, "mkt_cost_NN")
createOrReplaceTempView(mkt_cost_N, "mkt_cost_N")


model_score_all <- sql("
SELECT

  0 AS Score_SKey,
  SC.Individual_Original_SKey,
  SC.Organization_SKey,
  aarp AS AARP_amount,
  CASE
    WHEN SC.contract = 04500218 THEN MN.Score_Segment_SKey
    WHEN SC.contract NOT IN (04500218) THEN MNN.Score_Segment_SKey
  END AS Score_Segment_SKey,
  SC.AC_Product_Skey,
  prob_score AS Model_Calculation_Number,
  prob_score AS Adjusted_Model_Score,
  aarp AS Average_Annual_Renewal_Premium,
  CASE
    WHEN SC.contract = 04500218 THEN MN.mkt_cost_navy + MN.SA_Cost_pm_N +
      (prob_score * MN.Variable_Cost_pb)
    WHEN SC.contract NOT IN (04500218) THEN MNN.mkt_cost_nonnavy +
      MNN.SA_Cost_pm_NN + (prob_score * MNN.Variable_Cost_pb)
  END AS Marketing_Cost,
  prob_score * aarp AS Total_Annual_Renewal_Premium,
  CASE
    WHEN SC.contract = 04500218 THEN (prob_score * aarp) * PP.ppf_amount
    WHEN SC.contract NOT IN (04500218) THEN (prob_score * aarp) *  PP.ppf_amount
  END AS Potential_Profit,
  CASE
    WHEN SC.contract = 04500218 THEN ((prob_score * aarp) * PP.ppf_amount) - 
      (MN.mkt_cost_navy + MN.SA_Cost_pm_N + (prob_score * MN.Variable_Cost_pb))
    WHEN SC.contract NOT IN (04500218) THEN ((prob_score * aarp) * 
      PP.ppf_amount) - (MNN.mkt_cost_nonnavy + MNN.SA_Cost_pm_NN +
      (prob_score * MNN.Variable_Cost_pb))
  END AS Present_Value_Future_Profit
  
  FROM scr_ext AS SC
  INNER JOIN ppf_current AS PP ON
    SC.PPF_Prod_CD = PP.product_code_lookup_ppf AND
    SC.gendermfu_extract = PP.gendermfu_lookup_ppf AND
    SC.Automated_Payment_Flag = PP.Automated_Payment_Flag AND
    SC.billtype = PP.PPF_General AND
    SC.OrganizationPPFSegment = PP.organizationppfsegment
  INNER join mkt_cost_N MN ON
    SC.PPF_Prod_CD = MN.prod_code
  INNER join mkt_cost_NN MNN ON
    SC.PPF_Prod_CD = MNN.prod_code
")

persist(model_score_all, "MEMORY_ONLY")
createOrReplaceTempView(model_score_all, "model_score_all")

scr_ext <- get_ranks_via_case("model_score_all", "Present_Value_Future_Profit", 0.05, "dsc")
createOrReplaceTempView(scr_ext, "scr_ext")

scr_ext <- sql(paste0("
SELECT

  *,
  Model_Calculation_Number AS Model_Score,
  Present_Value_Future_Profit / (Present_Value_Future_Profit +
    Marketing_Cost) AS Marketing_Efficiency

FROM scr_ext
"))
createOrReplaceTempView(scr_ext, "scr_ext")

score_model_skey <- get_score_model_skey(hive_db, campaign_group, 134)

sql(paste0("
INSERT INTO TABLE ", target_db, ".score_fact
SELECT

  0 AS Score_SKey,
  individual_original_skey,
  organization_skey,
  ", score_model_skey, " AS Score_Model_SKey,
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
  ranks AS Pentile_number,
  NULL AS Auto_Quality_Score,
  NULL AS Student_Loan_Prospect_SKey,
  NULL AS Conversion_Score,
  NULL AS AUM_Score,
  NULL AS Retention_Score,
  NULL AS Retention_Pentile

FROM scr_ext
"))
