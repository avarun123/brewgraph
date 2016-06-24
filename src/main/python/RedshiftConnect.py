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

class RedshiftConnect:
    conn = None
    initDone=False
    def getConnectionString(self,user,pwd,dbname):
        connenction_string = "dbname='"+dbname+"' port='5439' user='"+user+"' password='"+pwd+"' host='sbux-redshift.cho75avhmqi9.us-west-2.redshift.amazonaws.com'";
        return connenction_string
    def __init__(self,user,pwd,dbname):
        if not self.conn :
            conn = psycopg2.connect(self.getConnectionString(user, pwd, dbname));
        self.initDone=True
    def execute(self,command):
        print('executing query '+command)
        cur = self.conn.cursor()
        cur.execute(command)
    def executeSqlQuery(self,query):
        print('executing query '+query)
        cur = self.conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        return rows
        
    def copyFromS3toRs(self,redshiftTable,s3Location,accessKeyId,accesskey,delimiter): 
        command = "copy "+str(redshiftTable)+" from "+"\'"+s3Location+"\' credentials "
        command+= "\'aws_access_key_id="+accessKeyId+";aws_secret_access_key="+accesskey+"\' delimiter \'"+delimiter+"\' IGNOREHEADER 1 ENCODING UTF8 NULL AS 'NULL'"
        print ("executing "+command)
        self.execute(command)
        self.conn.commit(); 
        
    def copyFromRstoS3(self,query,s3Location,accessKeyId,accesskey,delimiter): 
        command = "unload '"+query+"' to '"+s3Location+"\' credentials "
        command+= "\'aws_access_key_id="+accessKeyId+";aws_secret_access_key="+accesskey+"\' delimiter \'"+delimiter+"\' ENCODING UTF8 NULL AS 'NULL'"
        print ("executing "+command)
        self.execute(command)
        self.conn.commit();
        
    def close(self):   
        self.conn.close();
    
