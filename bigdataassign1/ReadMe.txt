Each folder in the zip file has The jar file, .java file and output files for the respective question.
To run each question go to the specific folder and perform the following operations:

========================================================================
Question 1:

File :	assign1.jar

Steps to run jar file:

1. Launch terminal at respective directory
2. Delete output directory - "/user/user_name/output1" if it already exists
3. Type the following command: hadoop jar assign1.jar assign11 /user/user_name/input/soc-LiveJournal1Adj.txt /user/user_name/output1
4. Output is at : output1/part-r-00000 
5. The output for two friends as said in problem is hard-coded in the .java file.

You will get output as : 
0,4	8,14,15,18,27,72,80,74,77
1,29826	
20,22939	1,5
28041,28056	6245,28054,28061
6222,19272	19263,19280,19281,19282



========================================================================

Question 2:

File: assign12.jar

Steps to run jar file:

1. Launch terminal at respective directory
2. Delete output directory - "/user/user_name/output2a","/user/user_name/output2b" if it already exists
3. Type the following command: hadoop jar assign1.jar q2 /user/user_name/input/soc-LiveJournal1Adj.txt output2a output2b
4. Output is at : "/user/user_name/output2a/part-r-00000" and "/user/user_name/output2b/part-r-00000"
5. 2 output files are generated- output2a is input for the second MapReduce task and gives final- output2b as output.

========================================================================

Question 3:

File: assign1.jar

Steps to run jar file:

1. Launch terminal at respective directory
2. Delete output directory - "/user/user_name/output3" if it already exists
3. Type the following command: hadoop jar assign13.jar assign1_3 /user/user_name/input/numbers.txt /user/user_name/output3
4. Output is at : output3/part-r-00000 

========================================================================

Question 4:

File: assign1.jar

Steps to run jar file:

1. Launch terminal at respective directory
2. Delete output directory - "/user/user_name/output4" if it already exists
3. Type the following command: hadoop jar assign14.jar assign1_4 /user/user_name/input/numbers.txt /user/user_name/output4
4. Output is at : output4/part-r-00000 

========================================================================
Question 5:

File: assign1.jar

Steps to run jar file:

1. Launch terminal at respective directory
2. Delete output directory - "/user/user_name/output5" if it already exists
3. Type the following command: hadoop jar assign11.jar  assign1_5 /user/user_name/input/numbers.txt /user/user_name/output5