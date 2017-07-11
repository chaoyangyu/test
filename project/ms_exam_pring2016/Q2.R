require(reshape2)
Q2 <- read.csv("~/Dropbox/ms exam spring2016/Q2.csv")

#Making FA_FX table
Q2_1 <-melt(Q2,measure.vars =c("v1_FA_FX","v2_FA_FX","v3_FA_FX","v4_FA_FX","v5_FA_FX"),
            variable.name = "Time", 
            value.name = "FA_FX")
temp=Q2_1[,c(81,82)]
Q2_2 <-melt(Q2, measure.vars = c("v1_MRIage_at_appointment","v2_MRIage_at_appointment",
                                   "v3_MRIage_at_appointment","v4_MRIage_at_appointment",
                                   "v5_MRIage_at_appointment"),
                   value.name = "Age")
Q2_2<-data.frame(Q2_2,temp)
keep<-c("Reggieid",	"sex",	"apoe4pos",	"FamilyHistory",
        "TotalYearsEducation",	"readstn",
        "v1_Petage_at_appointment",	"v1_PIB_8ROI",
        "v2_Petage_at_appointment","v2_PIB_8ROI","Time","FA_FX","Age")
Q2_FA_FX <- Q2_2[,names(Q2_2) %in% keep]
Q2_FA_FX<- Q2_FA_FX[complete.cases(Q2_FA_FX[,13]),]
Q2_FA_FX$FamilyHistory<-factor(Q2_FA_FX$FamilyHistory)
Q2_FA_FX$apoe4pos<-factor(Q2_FA_FX$apoe4pos)
Q2_FA_FX$sex<-factor(Q2_FA_FX$sex)
Q2_FA_FX$Reggieid<- as.character(Q2_FA_FX$Reggieid)


#function for making table
maketable = function(a,b,c,d,e,f){
  Q2_1 <-melt(Q2,measure.vars =c(a,b,c,d,e),
              variable.name = "Time", 
              value.name = f)
  temp=Q2_1[,c(81,82)]
  Q2_2 <-melt(Q2, measure.vars = c("v1_MRIage_at_appointment","v2_MRIage_at_appointment",
                                   "v3_MRIage_at_appointment","v4_MRIage_at_appointment",
                                   "v5_MRIage_at_appointment"),
              value.name = "Age")
  Q2_2<-data.frame(Q2_2,temp)
  keep<-c("Reggieid",	"sex",	"apoe4pos",	"FamilyHistory",
          "TotalYearsEducation",	"readstn",
          "v1_Petage_at_appointment",	"v1_PIB_8ROI",
          "v2_Petage_at_appointment","v2_PIB_8ROI","Time",f,"Age")
  Q2_FA_FX <- Q2_2[,names(Q2_2) %in% keep]
  Q2_FA_FX<- Q2_FA_FX[complete.cases(Q2_FA_FX[,13]),]
  Q2_FA_FX$FamilyHistory<-factor(Q2_FA_FX$FamilyHistory)
  Q2_FA_FX$apoe4pos<-factor(Q2_FA_FX$apoe4pos)
  Q2_FA_FX$sex<-factor(Q2_FA_FX$sex)
  Q2_FA_FX$Reggieid<- as.character(Q2_FA_FX$Reggieid)
  Q2_FA_FX = na.omit(Q2_FA_FX)
  Q2_FA_FX$amyloid.diff=Q2_FA_FX$v2_PIB_8ROI-Q2_FA_FX$v1_PIB_8ROI
  Q2_FA_FX$Age= scale(Q2_FA_FX$Age,scale=FALSE)
  return (Q2_FA_FX)
}





temp <- ave(data$FA_FX, factor(df$Reggieid), FUN=function(x) c(NA,diff(x)))


library(lattice)
temp = c("5583", "2933", "1459", "1559", "1827", "2175" ,"3099", "4586" ,"3658")


xyplot(MD_FX~Age | Reggieid,data=data[data$Reggieid %in% temp, ],
       panel=function(x,y, subscripts){panel.xyplot(x, y, pch=16)
         panel.lmline(x,y, lty=4)
        # panel.xyplot(Q2_FA_FX$Age[subscripts], y, pch=3)
         #panel.lmline(data$Age[subscripts],y) 
         }, 
       ylim=c(0.5, 2.5), as.table=T, subscripts=T)




#plots
require(ggplot2)
unique(data$Reggieid)
temp = c("5583", "2933", "1459", "1559", "1827", "2175" ,"3099", "3113" ,"3658")
ggplot(data[data$Reggieid %in% temp,], 
       aes(x=Age, y=FX_FA,group=Reggieid)) + 
  geom_point()+ 
  stat_summary(fun.y=mean, geom="line", aes(group=Reggieid))+
  facet_wrap(~Reggieid)+
  ggtitle("Change in FA_FX")


ggplot(data,
       aes(x=Age, y=FA_FX,group=Reggieid,colour=v1_PIB_8ROI)) + 
  geom_point()+ 
  geom_smooth(se=FALSE,method='lm')+
  #stat_summary(fun.y=mean, geom="line", aes(group=Reggieid))+
  facet_grid(FamilyHistory~apoe4pos,labeller=label_both)+

  ggtitle("Change in FA_FX")



ggplot(data, aes(x=Age, y=FA_FX,group=Reggieid,colour=TotalYearsEducation))+
  geom_smooth(se=FALSE,method='lm')+
  ggtitle("Change in FA_FX")


ggplot(Q2_FA_FX, aes(x=Time, y=FA_FX)) + geom_boxplot()+facet_grid(apoe4pos~FamilyHistory)+ 
  scale_x_discrete("FamilyHistory") +
  scale_y_continuous("apoe4posl")+
  ggtitle("Change in FA_FX")


temp=data.frame(matrix(c(Q2$v1_MD_FX,Q2$v2_MD_FX,Q2$v3_MD_FX,Q2$v4_MD_FX),ncol=4))
cor(temp,use="pairwise.complete.obs")



#Model
require(lme4)

data = maketable("v1_FA_CGH_R","v2_FA_CGH_R","v3_FA_CGH_R","v4_FA_CGH_R","v5_FA_CGH_R","FA_CGH_R")
data = maketable("v1_MD_CGH_L","v2_MD_CGH_L","v3_MD_CGH_L","v4_MD_CGH_L","v5_MD_CGH_L","MD_CGH_L")

out2= lmer(FA_FX~
             #v1_PIB_8ROI+
             #sex+
             #readstn+
             #apoe4pos+
             #FamilyHistory+
             #amyloid.diff+
             TotalYearsEducation+
             Age+(Age|Reggieid),data = data,REML=FALSE)
out3 =lmer(FA_FX~
             #v1_PIB_8ROI+
             #sex+
             #readstn+
          #apoe4pos+
             #FamilyHistory+
             #amyloid.diff+
             TotalYearsEducation+
             Age+ Age:TotalYearsEducation+
           (Age|Reggieid),data = data,REML=FALSE)
anova(out2,out3)



out= lmer(MD_CGH_L ~ amyloid.diff + Age + Age:amyloid.diff +(Age|Reggieid),data = data)

summary(out)



aug.Pred<-augPred(out,primary = ~Age,level = 0:1,length.out=2)
plot(aug.Pred,layout=c(3,3,1))
    



qqnorm(resid(out2))
plot(resid(out2) ~ fitted(out2),main="residual plot")
abline(h=0)
plot(out3, resid(., scaled=TRUE) ~ fitted(.), abline = 0)
coefs <- data.frame(coef(summary(out)))
# use normal distribution to approximate p-value
coefs$p.z <- 2 * (1 - pnorm(abs(coefs$t.value)))
coefs


ggplot(data,aes(x=Age, y= FA_FX))+geom_point()+geom_smooth()+facet_grid(.~Time)


                        