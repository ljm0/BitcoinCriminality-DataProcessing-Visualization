import requests
import json
from bs4 import BeautifulSoup
import time
def search_connected(central_address,degree,file_dir,max_degree):
    time.sleep(0.1)
    session=requests.session()
    string_number=BeautifulSoup(session.get("https://www.blockchain.com/btc/address/"+central_address+"").content).find("td",{"id":"n_transactions"}).text
    if len(string_number)<1:
        time.sleep(1)
    string_number = BeautifulSoup(
        session.get("https://www.blockchain.com/btc/address/" + central_address + "").content).find("td", {
        "id": "n_transactions"}).text

    get_number_of_tx=int(string_number)
    print(get_number_of_tx)
    offset=0
    input_list_whole=[]
    while(offset<get_number_of_tx):
        reponse=session.get("https://www.blockchain.com/btc/address/"+central_address+"?api_code=1389d9b7-4f15-4530-9ec1-a3d6890b6ea7&offset="+str(offset))
        input_list_whole.extend(paser_page(reponse.text))
        offset+=50
    if degree==max_degree:
        f=open(file_dir+"/"+str(degree),"a+")
        f.write(str((central_address,input_list_whole))+"\n")
        f.close()
        return (central_address,input_list_whole)
    else:
        more_deeper_list=[]
        #print(input_list_whole)
        f = open(file_dir + "/" + str(degree), "a+")
        f.write(str((central_address, input_list_whole))+"\n")
        f.close()
        for one_transaction_address_list in input_list_whole:
            for address in one_transaction_address_list[0]:
                more_deeper_list.extend(search_connected(address,degree+1,file_dir,max_degree))
        return more_deeper_list
def paser_page(text):
    soup=BeautifulSoup(text)
    input_list=[]
    for transaction in soup.find_all("div",{"class":"txdiv"}):
        #print(transaction)
        addresses_list=transaction.find_all("a")
        addresses_list=[address.text for address in addresses_list[1:]]
        if len(addresses_list)<1:
            continue
        time=transaction.find("span",{"class":"pull-right"}).text
        value=transaction.find_all("span")[2].text
        #print(addresses_list,time,value)
        input_list.append((addresses_list,time,value))
    return input_list
    #print(soup.find_all("div",{"class":"txdiv"}))