# kingcountyhomes
Model for predicting home prices 
Goals
* Personal project for using Spark
* examine trends

http://seattlebubble.com/blog/2008/02/19/king-county-home-prices-1946-2007/


## Raw Data 
King County provides several csv files with sales and detailed information for homes and business properties since the early 1900s. 
http://info.kingcounty.gov/assessor/DataDownload/default.aspx
Real Property Sales:http://aqua.kingcounty.gov/extranet/assessor/Real%20Property%20Sales.zip
Residential Building: http://aqua.kingcounty.gov/extranet/assessor/Residential%20Building.zip

## Processing
* Make unique pin for identifying select properties across csv files
* remove properties outside of WA
* consider only properties above 20,000
* narrow scope for 2005 to 2015 to study surges in pricing for recent times
