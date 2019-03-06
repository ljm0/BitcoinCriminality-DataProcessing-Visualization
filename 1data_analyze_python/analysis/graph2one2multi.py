import os
#import main_final
import central_points
f=open("../address_format.log",'r')
name_address_dic={}
for i in f.readlines():
    if len(i)>2:
        #print(i)
        tokens=i.strip("\n").split(",")
        address=tokens[0]
        type=tokens[1]
        tag=tokens[2]
        name_address_dic[type+"_"+tag+".gpickle"]=address
#print(name_address_dic)


f.close()
f=open("../valid negative",'r')
negative_map={}
for i in f.readlines():
    if len(i)>2:
        (Hash,address)=i.strip("\n").split(',')
        negative_map[Hash]=address
#print(negative_map)
Header = ['Total addresses', 'Total Transaction', 'Normality check(p-value)', 'Significance test(p-value)',
          'Percentage of cluster coefficient>0 addresses', 'Percentage of page Rank >0.01 address',
          'Average cluster coefficient of the whole graph', 'Percentage of in centrality>0.01 addresses',
          'Percentage of out centrality>0.01 addresses', 'Transitivity', 'Assortativity', 'Chordal',
          'Strongly connected',
          'Weakly connected', 'Global efficiency', 'Is branching', 'Immediate dominator(numbers)', 'Is simple path',
          'Reciprocity',
          'Max value', 'Min value', 'Average value', 'Median value', 'One fourth value', 'Three fourth value',
          'The max degree of all addresses', 'Polynomial parameters',
          'Important nodes information', 'Important nodes start-time', 'Important nodes end-time',
          'Important nodes time difference', 'This node as from', 'This node as to',
          'This node connected with(as from)', 'This node connected with(as to)',"tag"]
positive_set=os.listdir("../structures/criminality")
negative_set=os.listdir("../structures/donor")
#print(positive_set)
#print(negative_set)
output_list=[]
for i in positive_set:
    #print(i)
    print(name_address_dic[i])
    tmp=central_points.central_points("../structures/criminality/"+i,name_address_dic[i],i,1)
    tmp['tag']=1
    print(tmp)
    output_list.append(tmp)
with open("one2multi_positive.txt","a+") as f:
    f.write(str(output_list))
output_list=[]
for j in negative_set:
    address=j.split('_')[1][:-8]
    print(negative_map[address])
    tmp=central_points.central_points("../structures/donor/"+j,negative_map[address],j,0)
    tmp['tag']=0
    output_list.append(tmp)
with open("one2multi_negative.txt","a+") as f:
    f.write(str(output_list))