import multiInOut
import os
transaction_list=[]
positive_list=["../ExtortionEmail","../Porn","../Sextortion",'../Ransomware']
for file in positive_list:
    for tag in os.listdir(file):
        #print(tag)
        transaction_list.extend(multiInOut.sumup(file,tag))
with open("positive_trans_number",'a+') as f:
    f.write(str(transaction_list))
transaction_list=[]
for tag in os.listdir("../donator2"):
    transaction_list.extend(multiInOut.sumup("../donator2",tag))
with open("negative_trans_number",'a+') as f:
    f.write(str(transaction_list))
