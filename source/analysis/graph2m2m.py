import multiInOut
import os
transaction_list=[]
positive_list=["../ExtortionEmail","../Porn","../Sextortion",'../Ransomware']
import os
f=open("../address_format.log",'r')
for i in f.readlines():
    if len(i)>2:
        #print(i)
        tokens=i.strip("\n").split(",")
        address=tokens[0]
        type=tokens[1]
        tag=tokens[2]
        print(tokens)
        if os.path.exists("../"+type+'/'+ tag):
            print(address)
            tmp=multiInOut.handle_graph("../"+type, tag,address)
            tmp['tag']=1
            transaction_list.append(tmp)
f.close()
with open("positive_multi_number",'a+') as f:
    f.write(str(transaction_list))
transaction_list=[]
f=open("../valid negative",'r')
negative_map={}
for i in f.readlines():
    if len(i)>2:
        (Hash,address)=i.strip("\n").split(',')
        negative_map[Hash]=address
f.close()
for tag in os.listdir("../donator2"):
    print(tag)
    tmp = multiInOut.handle_graph("../donator2",tag,negative_map[tag])
    tmp['tag'] = 0
    transaction_list.append(tmp)
with open("negative_multi_number",'a+') as f:
    f.write(str(transaction_list))
