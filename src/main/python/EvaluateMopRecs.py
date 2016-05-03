'''
Created on Apr 28, 2016

@author: aveettil
'''
import requests;
import random

#mopUrl = "http://localhost:8080/brewgraphapi/BrewGraphServlet"
mopTestFile = "/projects/mop/mop-test-pos-basket.csv"
recCountStatus_twoitem={}
recCountStatus_threeitem={}
total_count_twoitem = 0
total_count_threeitem = 0
ranking_score_twoItem = 0;
ranking_score_threeItem = 0;
#testOutFile = "/projects/test/mopTest_python.csv"
#out = open(testOutFile,'w')
class BrewGraphEvaluator(object):
    '''
    classdocs
    '''
    def printStats(self,statusmap,count,rankingscore): 
        #sum = 0   
        #for k,v in statusmap.items():
        #    sum+=v
        for k,v in statusmap.items():
            #sum+=v
            print(k,v/count,v,count,rankingscore/count)
    def runTest(self):
        total_count_twoitem = 0
        total_count_threeitem = 0
        ranking_score_twoItem = 0;
        ranking_score_threeItem = 0;
        f = open(mopTestFile,"r")
        previousTxid = None;
        previousLine = "";
        currentLine = None;
        basket = set()
        lineNum=0
        for line in f:
            lineNum+=1
            if lineNum % 100000 == 0:
                print (lineNum)
                evaluator.printStats(recCountStatus_twoitem, total_count_twoitem,ranking_score_twoItem)
                #evaluator.printStats(recCountStatus_threeitem, total_count_threeitem)
            currentLine = line.split(",");
            txid = currentLine[0];
            current_custid=currentLine[3];
            current_timeOfDay = currentLine[6].strip('\n');
            current_item =  currentLine[4];
            if not current_item:
                continue
            recCountStatus = None
            recCount = None
            if (not (previousTxid is None)) and (previousTxid != txid):
                previous_country = previousLine[1];
                previous_region = previousLine[2]; 
                previous_custid=previousLine[3];
                previous_timeOfDay = previousLine[6].strip('\n');
                previous_item =  previousLine[4];
                if(len(basket) <=3 and len(basket) > 1) :
                    
                    '''small=10000
                    for item in basket:
                        if int(item) < int(small) :
                            small = item
                    basket.remove(small)
                    itemToTest = small'''
                    itemToTest = basket.pop()
                   # rest is seed
                    mopUrl="http://localhost:8080/brewgraphapi/BrewGraphServlet?country="+previous_country+"&region="+previous_region+"&custid="+previous_custid+"&timeofday="+previous_timeOfDay+"&seeds="+(";".join(basket))+"&personalize=true&pair=true&format=json"
                    r = requests.get(mopUrl)
                   
                    recs = r.text.split(";")
                    #print(r.text,total_count_twoitem,recCountStatus_twoitem)
                    if r.status_code==200 and len(recs) > 0:
                        
                        pos=0
                        if len(basket) == 1: #corresponds to 2 item basket after poping one out
                            recCountStatus = recCountStatus_twoitem
                            total_count_twoitem+=1
                        else:
                            recCountStatus = recCountStatus_threeitem
                            total_count_threeitem+=1
                        
                        found = False;     
                        for item in recs:
                           pos+=1
                           if item == itemToTest :
                                found = True
                                if pos in  recCountStatus:
                                    recCountStatus[pos]+=1 
                                    if len(basket) == 1:
                                       ranking_score_twoItem+=1/pos
                                    else:
                                       ranking_score_threeItem+=1/pos 
                                else:
                                   recCountStatus[pos]=1 
                                   
                                break
                        if not found:
                            if 0 in  recCountStatus:
                                recCountStatus[0]+=1 
                            else:
                                recCountStatus[0]=1 
                        ''' if  1 in recCountStatus_twoitem:   
                            out.write(str(itemToTest)+","+str(recs[0])+","+ str(total_count_twoitem)+","+str(recCountStatus_twoitem[1])+"\n")
                        
                        out.flush()#   print("not found\n")
                        '''
                basket = set()
            
            basket.add(current_item);
            previousTxid = txid;
            previousLine = currentLine;
            
       
            
    
if __name__ == "__main__":
    
    evaluator = BrewGraphEvaluator()
    evaluator.runTest()
    evaluator.printStats(recCountStatus_twoitem, total_count_twoitem,ranking_score_twoItem)
    evaluator.printStats(recCountStatus_threeitem, total_count_threeitem,ranking_score_threeItem)
    