1. The file `summary-report.R` is run automatically after score-fact table is created in production and 
   demo runs. It summarizes count, mean and median information of columns from the score_fact table.
2. The file `compare-report.R` is used to compare summary reports from two campaigns in order to generate 
   the final validation report. At this point, `compare-report.R` needs to be run manually.