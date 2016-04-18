This is scala code to compute Jaccard coefficient between multiple product items from POS.
Input is a csv file with the format country,region,guid,productid,count of pos transactions,time of day.

count is group by  country,region,guid,productid,timeofday. Time of day is broken into morning(until 11 am), midday(11 am to 2.30 pm),evening(2.30 pm to 6PM) and night (6PM and later)

To build
========
install maven.

> cd <git project folder>
> mvn install



to run
====
cd to <git project folder>/target directory
you will see a file called brewgraph-1.0.jar

copy  brewgraph-1.0 jar to bda1node01 using scp.

from bda1node01 run
 nohup spark-submit --class com.sbux.loyalty.brewgraph.main.Main --master yarn-client --deploy-mode client --driver-memory 20g --executor-memory 15g --num-executors 6 --executor-cores 4 --conf  spark.kryoserializer.buffer.max=512m --conf  spark.driver.maxResultSize=10g /home/aveettil/brewgraph-1.0.jar hdfs:///user/aveettil/brewgraphin hdfs:///user/aveettil/brewgraph-out > out1.txt &

