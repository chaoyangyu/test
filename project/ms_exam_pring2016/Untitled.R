out2= lmer(MD_CGH_L~
             #v1_PIB_8ROI+
             #sex+
             #readstn+
             #apoe4pos+
             #FamilyHistory+
             #TotalYearsEducation+
             amyloid.diff+
             Age+
             #Age:amyloid.diff+
             #Age:amyloid.diff:apoe4pos+
             #Age:v1_PIB_8ROI+
             #Age:sex+
             #Age:readstn+
             #Age:apoe4pos+
             #Age:FamilyHistory+
             #Age:TotalYearsEducation+
             Age:amyloid.diff+
             (Age|Reggieid),
             data = data,REML=FALSE)
out3 =lmer(MD_CGH_L~
             #v1_PIB_8ROI+
             #sex+
             #readstn+
             #apoe4pos+
             FamilyHistory+
             #TotalYearsEducation+
             amyloid.diff+
             Age+
             #Age:v1_PIB_8ROI+
             #Age:sex+
             #Age:readstn+
             #Age:apoe4pos+
             Age:FamilyHistory+
             #Age:TotalYearsEducation+
             Age:amyloid.diff+
             #Age:amyloid.diff:FamilyHistory+
             (Age|Reggieid),data = data,REML=FALSE)
anova(out2,out3)
