import entrance
f=open("address_format_ransomware","r")
address_set=set([i[:-1] for i in f.readlines()])
#print(address_set)
for address in address_set:
    #print(address)
    central_address,type_name,tag=address.split(',')
    entrance.download_per(central_address,type_name,tag,2)