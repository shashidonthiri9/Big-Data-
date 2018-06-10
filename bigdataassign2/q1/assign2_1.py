import sys
from pyspark import SparkContext, SparkConf

def create_mutual_friends(line):
    final_friend_list = []
    candidate_beforeTab = line[0].strip()
    list_ofHisFriends = line[1]
    if(candidate_beforeTab != ''):
        candidate_beforeTab = int(candidate_beforeTab)    
        for eachfriend in list_ofHisFriends:
            eachfriend = eachfriend.strip()
            if(eachfriend != ''):
                eachfriend = int(eachfriend)
                if(int(eachfriend) < int(candidate_beforeTab)):
                    val = (str(eachfriend)+","+str(candidate_beforeTab),set(list_ofHisFriends))
                else:
                    val = (str(candidate_beforeTab)+","+str(eachfriend),set(list_ofHisFriends))
                final_friend_list.append(val)
        print(type(final_friend_list))
        return(final_friend_list)

def _mapper(line):    
    _key = line[0]
    _value = list(line[1])
    s_string = ",".join(_value)
    return("{0}\t {1}".format(_key,s_string))

if __name__ == "__main__":
    #DriverCode
    conf = SparkConf().setAppName("mutualfriends").setMaster("local[2]") #sparkConfig
    spark_cont = SparkContext.getOrCreate(conf = conf) 
    
    input_mutualfriends = spark_cont.textFile("/FileStore/tables/soc-LiveJournal1Adj.txt/soc_LiveJournal1Adj-2d179.txt") #inputFile
    
    lines_split = input_mutualfriends.map(lambda x : x.split("\t")).filter(lambda x : len(x) == 2).map(lambda x: [x[0],x[1].split(",")])
    #input line is filtered and displayed in arrays where for each line in input file, arr[0] is the first value before tab and arr[1] 
    #has values that are his/her friends. 
    
    
    mutual_friends_FMap = lines_split.flatMap(create_mutual_friends) #flatmap vs map
    #flatmap returns result in arrays or lists instead of arrays of arrays i.e; basically it flattens the resultset
    
    RDD_reducer = mutual_friends_FMap.reduceByKey(lambda x,y: x.intersection(y))
    
    RDD_result = RDD_reducer.map(_mapper)
    RDD_result.coalesce(1).saveAsTextFile("/FileStore/tables/ques_1/checkresult")