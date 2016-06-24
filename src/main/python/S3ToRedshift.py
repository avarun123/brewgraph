'''
Created on Apr 28, 2016

@author: aveettil
'''
import requests;
import random
import sys, getopt
from boto.beanstalk.response import S3Location
from subprocess import call
import psycopg2;
from RedshiftConnect import RedshiftConnect

def main(argv):
    source = ""
    dest=""
    query=""
    accessKeyId=""
    accesskey=""
    delimiter='\t'
    user=""
    pwd=""
    #dbName=
    try:
      opts, args = getopt.getopt(argv,"hs:t:i:k:d:u:p:q:",["src=","dest=","id=","key=","del=","user=","pwd=","query="])
    except getopt.GetoptError:
      print ('S3ToRedshift.py -t <redshift table name> -s <s3 location> -i <aws access key id> -k <aws access key>' )
      sys.exit(2)
    for opt, arg in opts:
      if opt == '-h':
         print ('S3ToRedshift.py -t <redshift table name> -s <s3 location> -i <aws access key id> -k <aws access key>' )
         sys.exit()
      elif opt in ("-s", "--src"):
         source = arg
      elif opt in ("-t", "--dest"):
         dest = arg
      elif opt in ("-i", "--id"):
         accessKeyId = arg
      elif opt in ("-k", "--key"):
         accesskey = arg
      elif opt in ("-d", "--del"):
        delimiter = arg
      elif opt in ("-u", "--user"):
        user = arg
      elif opt in ("-p", "--pwd"):
        pwd = arg
      elif opt in ("-q", "--query"):
        query = arg
     
     
    
    redshift =   RedshiftConnect(user,pwd,'dev') 
    if "s3:" in source :
        redshift.copyFromS3toRs(dest, source, accessKeyId, accesskey, delimiter)
    elif "s3:" in dest:
        redshift.copyFromRstoS3(query, dest, accessKeyId, accesskey, delimiter)   
    redshift.close()

    #call([command, "-l"])
if __name__ == "__main__":
   main(sys.argv[1:])
    
    