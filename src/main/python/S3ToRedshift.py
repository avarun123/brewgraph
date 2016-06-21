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
def main(argv):
    s3Location = ""
    redshiftTable=""
    accessKeyId=""
    accesskey=""
    delimiter='\t'
    user=""
    pwd=""
    #dbName=
    try:
      opts, args = getopt.getopt(argv,"hs:t:i:k:d:u:p:",["src=","table=","id=","key=","del=","user=","pwd="])
    except getopt.GetoptError:
      print ('S3ToRedshift.py -t <redshift table name> -s <s3 location> -i <aws access key id> -k <aws access key>' )
      sys.exit(2)
    for opt, arg in opts:
      if opt == '-h':
         print ('S3ToRedshift.py -t <redshift table name> -s <s3 location> -i <aws access key id> -k <aws access key>' )
         sys.exit()
      elif opt in ("-s", "--src"):
         s3Location = arg
      elif opt in ("-t", "--table"):
         redshiftTable = arg
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
           
    command = "copy "+str(redshiftTable)+" from "+"\'"+s3Location+"\' credentials "
    command+= "\'aws_access_key_id="+accessKeyId+";aws_secret_access_key="+accesskey+"\' delimiter \'"+delimiter+"\' IGNOREHEADER 1 ENCODING UTF8 NULL AS 'NULL'"
    print ("executing "+command)
    
    connenction_string = "dbname='dev' port='5439' user='"+user+"' password='"+pwd+"' host='http://sbux-redshift.cho75avhmqi9.us-west-2.redshift.amazonaws.com'";
    print ("Connecting to \n   " +connenction_string)
    conn = psycopg2.connect(connenction_string);
    cur = conn.cursor()
    cur.execute(command)
   # cur.execute("select * from ")
# DO THE DEW :)

    conn.commit();
    conn.close();

    #call([command, "-l"])
if __name__ == "__main__":
   main(sys.argv[1:])
    