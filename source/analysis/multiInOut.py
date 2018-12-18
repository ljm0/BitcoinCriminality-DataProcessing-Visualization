def parser(type_name,tag,file_name):
    f=open(type_name+'/'+tag+'/'+str(file_name),"r")
    transaction_number_list=[]
    for transaction_string in f.readlines():
        #print(transaction_string)
        transaction=eval(transaction_string)
        inputs_number=len(transaction[0])
        outputs_number=len(transaction[1])
        #print(inputs_number)
        transaction_number_list.append((inputs_number,outputs_number,tag))
    return transaction_number_list
def sumup(type_name, tag):
    print(tag)
    (transaction_number_list0)=parser(type_name, tag, 1)
    (transaction_number_list1)=parser(type_name, tag, 2)
    #print(transaction_number_list1)
    return transaction_number_list0+transaction_number_list1

def parser_multi(type_name,tag,file_name,central,set25):
    f=open(type_name+'/'+tag+'/'+str(file_name),"r")
    if set25==None:
        out_dic={0:0,5:0}
        set2=set()
        set5=set()
        for transaction_string in f.readlines():
            # print(transaction_string)
            transaction = eval(transaction_string)
            inputs_number = len(transaction[0])
            outputs_number = len(transaction[1])
            if transaction[0]:
                inputs_address = [i[0] if i else None for i in transaction[0]]
            else:
                inputs_address=[]
            if transaction[1]:
                #print(transaction[1])
                outputs_address = [i[0] if i else None for i in transaction[1]]
            else:
                outputs_address=[]
            #print(inputs_address,outputs_address)
            if central in inputs_address:
                if inputs_number >= 5 and outputs_number >= 5:
                    out_dic[0] += 1
                    #print('---')
                set2 = set2.union(set(inputs_address))
                #print(set2)
            else:
                if inputs_number >= 5 and outputs_number >= 5:
                    out_dic[5] += 1
                set5 = set5.union(set(outputs_address))
                #print(set5)
        if len(set2):
            set2.remove(central)
        #print(set2,set5)
        return out_dic,(set2,set5)
    else:
        set2=set25[0]
        set5=set25[1]
        out_dic={2:0,5:0,4:0,6:0}
        for transaction_string in f.readlines():
            # print(transaction_string)
            transaction = eval(transaction_string)
            inputs_number = len(transaction[0])
            outputs_number = len(transaction[1])
            if transaction[0]:
                inputs_address = [i[0] if i else None for i in transaction[0]]
            else:
                inputs_address=[]
            if transaction[1]:
                #print(transaction[1])
                outputs_address = [i[0] if i else None for i in transaction[1]]
            else:
                outputs_address=[]
            if inputs_number>=5 and outputs_number>=5:
                if len(set2&set(inputs_address))>0:
                    if central not in outputs_address:
                        out_dic[2]+=1
                elif len(set5&set(inputs_address))>0:
                    if central not in outputs_address:
                        out_dic[5]+=1
                if len(set2&set(outputs_address))>0:
                    out_dic[4]+=1
                elif len(set5&set(outputs_address))>0:
                    out_dic[6]+=1
        return out_dic

def handle_graph(type_name,tag,central):
    (dic_1,set_25)=parser_multi(type_name,tag,1,central,None)
    #print(len(set_25[0]),len(set_25[1]))
    dic_2=parser_multi(type_name,tag,2,central,set_25)
    out_dic={}
    out_dic[0]=dic_1[0]
    out_dic[5]=dic_1[5]+dic_2[5]
    out_dic[2]=dic_2[2]
    out_dic[4]=dic_2[4]
    out_dic[6]=dic_2[6]
    return out_dic