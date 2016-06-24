import requests;
import random
import sys, getopt
 
 
from RedshiftConnect import RedshiftConnect
 
from datetime import datetime
def getQueryStringFromFile(file):
    f = open(file,'r')
    query=""
    for line in f:
          query+=line
    return query
def getStartDate(redshift,period):
    query = getQueryStringFromFile('../sql/getLatestTxDate.sql')
    rows = redshift.executeSqlQuery(query)
    latestTxDate=""
    for row in rows:
        latestTxDate=row[1]
    date_object = datetime.strptime(latestTxDate, '%Y-%m-%d')
    
    startDate = date_object - datetime.timedelta(days=int(period))    
    return startDate.strftime('%Y-%m-%d')
def main(argv):
    table="mop_brewgraph_input"
    period="30" # number of days to look back
    user=""
    pwd=""
    #dbName=
    try:
      opts, args = getopt.getopt(argv,"hd:t:i:k:u:p:",["dest=","time=","id=","key=","user=","pwd="])
    except getopt.GetoptError:
      print ('CreateBrewgraphInput.py -t <redshift table name> -p <number of days to process> -i <aws access key id> -k <aws access key>' )
      sys.exit(2)
    for opt, arg in opts:
      if opt == '-h':
         print ('CreateBrewgraphInput.py -t <redshift table name> -p <number of days to process> -i <aws access key id> -k <aws access key>' )
         sys.exit()
      elif opt in ("-d", "--dest"):
         table = arg
      elif opt in ("-t", "--time"):
         period = arg
      elif opt in ("-i", "--id"):
         accessKeyId = arg
      elif opt in ("-k", "--key"):
         accesskey = arg
      
      elif opt in ("-u", "--user"):
        user = arg
      elif opt in ("-p", "--pwd"):
        pwd = arg
       
      
     
    
    redshift =   RedshiftConnect(user,pwd,'dev') 
    query = redshift.executeSqlQuery('../sql/createBrewGraphInput')
    startDate = getStartDate(redshift,period)
    query=query.replace('$a.ci2_trans_dt',startDate)
    query=query.replace('$mop_brewgraph_input',table)
    redshift.executeSqlQuery(query)
    redshift.close()

    #call([command, "-l"])
if __name__ == "__main__":
   main(sys.argv[1:])
    
    