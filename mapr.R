
library(rmr2)
rmr.options(backend.parameters = list(

hadoop = list(D = "mapreduce.map.memory.mb=1024")

))
library(rhdfs)
hdfs.init()
tsv.format <- make.input.format("csv", sep=",")
csv.format <- make.output.format("csv", sep=",")
input <- '/user/bedrock/data/temp/tempData.csv'
now<-Sys.time()
output <- paste0('/user/bedrock/data/output/',format(now, "%Y%m%d_%H%M%S_"))
out.ptr <- mapreduce(input, input.format=tsv.format,
 output=output, output.format=csv.format,
 map=function(k,v) {
f<-c(v[2],v[3],v[4],v[5],v[6],v[7],v[8],v[9],v[10],v[11],v[12],v[13]) 
keyval(v[1],sapply(f,mean))
 },
 reduce=function(k,v) {
   keyval(k, v)
 })
result <- from.dfs(out.ptr, format="csv")
#stripchart(result$vals,method="jitter") 
#ch = rawToChar(result)
file<-paste0(out.ptr,"/part-00000")
f = hdfs.file(file,"r")
m = hdfs.read(f)
c = rawToChar(m)
data = read.table(textConnection(c), sep = ",")
setwd("/home/bedrock/ninja")
jpeg("hist3.jpeg")
hist(data$V2,main="Frequency of temperature range",xlab="Temperature")
print("Histogram is produced at ")
print(getwd())
dev.off()
