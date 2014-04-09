######################
#Read the stock EOD from MySql for an year
#Assemble the features Open-Close,Open-High,Open-Low
#Detect Outlier using LoF by running the spread data aganist
#Volumes
#Dump all the eod, which has a score beyond a threshold default is 4
#####################
library("RMySQL")
library("quantmod")
library("DMwR") #data mining with R, provides LoF Impl

user="root"
password="India"
db="Stocks"
tableName="StockEOD"
outFileName="outlierscores.csv"
fromDate="2013-01-01"
toDate="2014-12-31"


#get all symbols from DB
getSymbolsFromDB<-function(){
  query<-paste("select Distinct(Symbol) from ",  tableName, "  Order by Sector",sep="")
  symbols <- dbGetQuery(dbEnv$dbConn,query)
  
}



detectOutliers<-function(symbol,fromData,toDate){
  
  query<-paste("select FULLTS as Date ,Open,High,Volume,Low,Close FROM " , tableName, " WHERE FULLTS BETWEEN '"
               ,fromDate, "'"," AND " ,"'",toDate ,"'"," AND Symbol=" ,"'",symbol,"'", " AND VOLUME!=0" ," ORDER BY FULLTS ASC",sep="")
  
  stockData <- dbGetQuery(dbEnv$dbConn,query)
  # cat("Rows Extracted for ",symbol, " count " , nrow(stockData),"\n")
  
  
  if (!is.null(stockData)){
    
    if (nrow(stockData) > 120){
      zz <- read.zoo(stockData, sep = ",",format="%Y-%m-%d", header=TRUE,index.column=1)
      
      highLowSpread <- (stockData$High - stockData$Low)
      openCloseSpread <- stockData$Open - stockData$Close
      openHighSpread <- stockData$Open - stockData$High
      openLowSpread <- stockData$Open - stockData$Low
      #need to store the date as another col
      dateIdx<-index(zz)
      #assemble features
      zz$HighLow<-highLowSpread
      zz$OpenClose<-openCloseSpread
      
      zz$OpenHigh<-openHighSpread
      zz$OpenLow <- openLowSpread
      #zz$Date<-dateIdx
      
      
      cols<-c("Volume","HighLow","OpenClose", "OpenHigh", "OpenLow")
      df<-data.frame(zz[,cols])
      orderScores<-data.frame(lofOutlierDetection(df))
      
      #TODO:knock of Inf nd NA fields
      scoredHigh <-orderScores[which(orderScores$Scores > 4), ]
      
      if (nrow(scoredHigh) > 0){
        dateIds<-scoredHigh$ItemIdx
        dates<-dateIdx[dateIds]
        
        scoredHigh$Date <-dates
        scoredHigh$Symbol <- symbol
        cat("Number of outliers " ,(nrow(scoredHigh)) , " for " , symbol, sep=" ","\n");  
        
        StockDataEnv$symbolCount<-StockDataEnv$symbolCount+nrow(scoredHigh)
        #load into output frame
        StockDataEnv$outDf<-rbind(scoredHigh,StockDataEnv$outDf)
      }
      
      
    }
    
  }
  
}


lofOutlierDetection<-function(priceChangeDf){
  #scale the fields 
  scaledDf <- scale(priceChangeDf)
  
  dateIdx<-index(scaledDf)
  scaledPriceMat<-as.matrix(scaledDf)
  lofScores<-lofactor(scaledPriceMat,5)
  
  
  priceChangeDf$Scores <- lofScores
  priceChangeDf$ItemIdx<-dateIdx
  #sort on scores
  orderDf <- priceChangeDf[order(priceChangeDf$Scores,na.last = TRUE, decreasing = TRUE),]
  orderDf
}




#fromD<-as.Date('2001-01-01',format='%Y-%m-%d')
#toD<-as.Date('2001-01-01',format='%Y-%m-%d')

dbEnv <- new.env()
StockDataEnv <- new.env()


assign('dbConn', dbConnect(MySQL(),user=user,password=password,dbname=db), envir=dbEnv)
#assign('stockData', stckMoves, envir=StockDataEnv)

outDf<-data.frame()
symbolCount<-0
assign('outDf', outDf, envir=StockDataEnv)
assign('symbolCount', symbolCount, envir=StockDataEnv)


symbols<-getSymbolsFromDB()
assign('StockSymbols', symbols, envir=StockDataEnv)
cat("Total symbols are " , nrow(symbols), sep=" ","\n")

 for(i in 1:nrow(symbols) ){
  symbol <- symbols[i,]
  # a filter to quickly reach to intrested stock
  if (symbol == "SUNPHARMA"){
  str <- cat("Working on symbol " , symbol,sep=" ","\n")
  dataSet<-detectOutliers(symbol,fromDate,toDate)
  
  }
  #without sleep MySql overheats my machine, unsure if this is Rbug or MySql
  #Sys.sleep(1)
  
 # break
}
#remove the index not required
StockDataEnv$outDf$ItemIdx<-NULL

if (StockDataEnv$symbolCount > 0){
write.csv(format(StockDataEnv$outDf,digits=4),
          
         file=outFileName,row.names=FALSE,quote =FALSE)
}

on.exit(dbDisconnect(dbEnv$dbConn))



#dbDisconnect(dbEnv$dbConn)