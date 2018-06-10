import sys
from pyspark import SparkContext, SparkConf
def Result_FinapMap1(line):
    
    Numberof_Friends = str(line[1][0][0][1])
    Candidate1_data = line[1][0][1]
    Candidate2_data = line[1][1]
    
    user1_firstname = Candidate1_data[0]
    user1_lastname = Candidate1_data[1]
    user1_address = Candidate1_data[2]
    
    user2_firstname = Candidate2_data[0]
    user2_lastname = Candidate2_data[1]
    user2_address = Candidate2_data[2]
    
    return("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}".format(Numberof_Friends,user1_firstname,user1_lastname,user1_address,user2_firstname,user2_lastname,user2_address))
    
    
def Format1_user_data(line):
  Candidate1 = line[0]
  Candidate2 = line[1][0][0]
  Numberof_Friends = line[1][0][1]
  Candidate1_data = line[1][1]
  return( Candidate2,((Candidate1,Numberof_Friends),Candidate1_data) ) 
def format_data(line):
  line = line.split(",")
  return(line[0],(line[1],line[2],line[3]))
def split_top(line):
  Friends_pairs = line[0]
  Numberof_Friends = line[1]
  Friends_pairs = Friends_pairs.split(",")
  return(Friends_pairs[0],(Friends_pairs[1],Numberof_Friends))
  
def create_mutual_friends(line):
    
    Candidate = line[0].strip()
    ListofCandidateFriends = line[1]
    if(Candidate != ''):
        Candidate = int(Candidate)
        
        Final_List_ofFriends = []
        
        for eachFriend in ListofCandidateFriends:
            
            eachFriend = eachFriend.strip()
            
            if(eachFriend != ''):
                eachFriend = int(eachFriend)
                if(int(eachFriend) < int(Candidate)):
                    value_get = (str(eachFriend)+","+str(Candidate),set(ListofCandidateFriends))
                else:
                    value_get = (str(Candidate)+","+str(eachFriend),set(ListofCandidateFriends))
                
                Final_List_ofFriends.append(value_get) 
        return(Final_List_ofFriends)

def Map_final(line):
    
    _key = line[0]
    _value = list(line[1])
    s_string = ",".join(_value)
    return("{0}\t {1}".format(_key,s_string))

if __name__ == "__main__":
    config = SparkConf().setAppName("mutualfriends").setMaster("local[2]")
    sparkcont = SparkContext.getOrCreate(conf = config)
    
    input_mutualfriends = sparkcont.textFile("/FileStore/tables/q2_assign/soc_LiveJournal1Adj_txt-b8957.txt")
    lines_split = input_mutualfriends.map(lambda x : x.split("\t")).filter(lambda x : len(x) == 2).map(lambda x: [x[0],x[1].split(",")])
    
    split_mutualFriends = lines_split.flatMap(create_mutual_friends)
    
    Reducer_RDD = split_mutualFriends.reduceByKey(lambda x,y: x.intersection(y))
    #print(Reducer_RDD.first())
    Len_listofFriends = Reducer_RDD.mapValues(lambda x: len(x))
   # print(Len_listofFriends.first())
    List_OfsortedFriends = Len_listofFriends.sortBy(lambda x: -x[1])
    top_teninList = List_OfsortedFriends.take(10)
   # print(top_teninList)
    top_list = sparkcont.parallelize(top_teninList)
    pairs_data = top_list.map(split_top)
    
    user_data = sparkcont.textFile("/FileStore/tables/ques_1/userdata.txt")
    Format_user_data = user_data.map(format_data)
    
    Join_pairedFriends = pairs_data.join(Format_user_data)
    join_pairs_format = Join_pairedFriends.map(Format1_user_data)
    
    join_pairs_format1 = join_pairs_format.join(Format_user_data)
    RDD_result = join_pairs_format1.map(Result_FinapMap1)
    
    RDD_result.coalesce(1).saveAsTextFile("/FileStore/tables/ques_1/checkresult")





