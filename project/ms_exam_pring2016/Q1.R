Q1 <- read.csv("~/Desktop/ms exam spring2016/Q1.csv")

#Exploration
require(ggplot2)

ggplot(Q1[Q1$Year==2014,], aes(x=Time, y=Biomass,group=Reactor.Number,color=Sand)) + 
  geom_point() +
  stat_summary(fun.y=mean, geom="line", aes(group=Sand))  + 
  stat_summary(fun.y=mean, geom="point")+
  facet_grid(Water~.,scales = "free")


#Biomass

ggplot(Q1, aes(x=Time, y=Biomass, group=Water,colour=Water,shape = factor(Year)))+
  geom_point()+
  geom_smooth(method='lm',se=FALSE)+
  facet_grid(Sand~.,scales="free")+
  #stat_summary(fun.y=mean,geom="line",aes(group=Water))+
ggtitle("Biomass vs. Time")


#Respiration
ggplot(Q1, aes(x=Time, y=Respiration, group=Reactor.Number,colour=Water,shape=factor(Year)))+
  geom_point()+facet_grid(Sand~.,scales="free")+
  stat_summary(fun.y=mean,geom="line",aes(group=Water))+
  ggtitle("Respiration vs. Time")




summary(out)



require("lattice")
xyplot(Biomass~ Respiration |Sand, data=Q1, as.table=F)

library(GGally)
ggpairs(temp)
Time0=Q1$Biomass[which(Q1$Time=="0")]
Time7=Q1$Biomass[which(Q1$Time=="7")]
Time14=Q1$Biomass[which(Q1$Time=="14")]
Time21=Q1$Biomass[which(Q1$Time=="21")]
Time28=Q1$Biomass[which(Q1$Time=="28")]
Time35=Q1$Biomass[which(Q1$Time=="35")]

temp = data.frame(Time0,Time7,Time14,Time21,Time28,Time35)
temp = data.frame(matrix(c(Q1$Respiration[which(Q1$Time=="0")],Q1$Respiration[which(Q1$Time=="7")],
          Q1$Respiration[which(Q1$Time=="14")],Q1$Respiration[which(Q1$Time=="21")]
          ,Q1$Respiration[which(Q1$Time=="28")],Q1$Respiration[which(Q1$Time=="35")]),ncol=6))
cor(temp)


#Q1：
Q1$Sand2 <- factor(Q1$Sand, c("PZS","GWS","RWS"))
Q1$Water2 <- factor(Q1$Water,c("GW","AGW", "ARW" ,  "RW" ))

#Biomass
out=lm(Biomass ~ Sand2*Water2+factor(Year), data=Q1[Q1$Time==0,])

summary(out)
coefs <- data.frame(coef(summary(out)))
# use normal distribution to approximate p-value
coefs$p.z <- 2 * (1 - pnorm(abs(coefs$t.value)))
coefs
plot(out, resid(., scaled=TRUE) ~ fitted(.), abline = 0)

summary(lmer(Biomass ~ Sand*Water+factor(Year)+(1|Reactor.Number), data=Q1[Q1$Time==0,]))
summary(lm(Biomass ~ Sand2*Water2+factor(Year), data=Q1[Q1$Time==7,]))
summary(lm(Biomass ~ Sand2*Water2+factor(Year), data=Q1[Q1$Time==14,]))
summary(lm(Biomass ~ Sand2*Water2+factor(Year), data=Q1[Q1$Time==21,]))
summary(lm(Biomass ~ Sand2*Water2+factor(Year), data=Q1[Q1$Time==28,]))
summary(lm(Biomass ~ Sand2*Water2+factor(Year), data=Q1[Q1$Time==35,]))
summary(lm(Biomass ~ Sand2*Water2, data=Q1[Q1$Time==42,]))


#Respiration
out=lm(Respiration ~ Sand2*Water2+Year, data=Q1[Q1$Time==14,])

coefs <- data.frame(coef(summary(out)))
# use normal distribution to approximate p-value
coefs$p.z <- 2 * (1 - pnorm(abs(coefs$t.value)))
coefs
plot(out, resid(., scaled=TRUE) ~ fitted(.), abline = 0)

summary(lm(Respiration ~ Sand2*Water2+factor(Year), data=Q1[Q1$Time==0,]))
summary(lm(Respiration ~ Sand2*Water2+factor(Year), data=Q1[Q1$Time==7,]))
summary(lm(Respiration ~ Sand2*Water2+factor(Year), data=Q1[Q1$Time==14,]))
summary(lm(Respiration ~ Sand2*Water2+factor(Year), data=Q1[Q1$Time==21,]))
summary(lm(Respiration ~ Sand2*Water2+factor(Year), data=Q1[Q1$Time==28,]))
summary(lm(Respiration ~ Sand2*Water2+factor(Year), data=Q1[Q1$Time==35,]))
summary(lm(Respiration ~ Sand2*Water2, data=Q1[Q1$Time==42,]))



#Q2：



out1 = lm(Biomass~(Sand2*Water2)*Time + Sand2*Water2+factor(Year), data = Q1)
out2 = lm(Biomass~(Sand2+Water2)*Time + Sand2+Water2+factor(Year), data = Q1)

anova(out1,out2)

out = lm(Biomass ~ Sand2 + Time + factor(Year) + Sand2:Time,data=Q1)
summary(out)

out3= lm(Respiration~(Sand2*Water2)*Time + Sand2*Water2+factor(Year), data = Q1)
out4= lm(Respiration~(Sand2+Water2)*Time + Sand2+Water2+factor(Year), data = Q1)

step(out3)

out= lm(Respiration ~ Sand2 + Water2 + Time + factor(Year) + 
          Sand2:Water2 + Sand2:Time, data = Q1)

summary(out)



step(out1)
summary(out1)

plot(out2, resid(., scaled=TRUE) ~ fitted(.), abline = 0)
summary(out2)
coefs <- data.frame(coef(summary(out2)))
# use normal distribution to approximate p-value
coefs$p.z <- 2 * (1 - pnorm(abs(coefs$t.value)))
coefs



library(plyr)
agg = ddply(Q1, .(Sand2, Water2,Year), function(x){
  c(sd = sd(x$Respiration))
})




