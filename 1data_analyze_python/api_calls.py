import requests
import json
import time
def search_connected(session,central_address,degree,file_dir,max_degree):
    #print(central_address)
    session=requests.session()
    if central_address==None:
        return None
    time.sleep(0.5)
    try:
        data = session.get(
            "https://blockchain.info/rawaddr/" + central_address + "?api_code=1389d9b7-4f15-4530-9ec1-a3d6890b6ea7").text
        if data[0] == 'I' or data[0] == 'C':
            return None
        loaded_data=json.loads(data)

    except:
        time.sleep(5)
        data = session.get(
            "https://blockchain.info/rawaddr/" + central_address + "?api_code=1389d9b7-4f15-4530-9ec1-a3d6890b6ea7").text
        if data[0] == 'I' or data[0] == 'C':
            return None
        loaded_data = json.loads(data)

        #print(data)
    #print(loaded_data)
    number_of_tx=loaded_data['n_tx']
    offset=50
    f = open(file_dir + "/" + str(degree), "a+")
    whole_input_list_for_cenral=extract_tx(loaded_data,f)
    while(offset<number_of_tx):
        try:
            data = session.get(
                "https://blockchain.info/rawaddr/" + central_address + "?api_code=1389d9b7-4f15-4530-9ec1-a3d6890b6ea7&offset=" + str(
                    offset)).text
            loaded_data = json.loads(data)
        except:
            time.sleep(1)
            data = session.get(
                "https://blockchain.info/rawaddr/" + central_address + "?api_code=1389d9b7-4f15-4530-9ec1-a3d6890b6ea7&offset=" + str(
                    offset)).text
        loaded_data = json.loads(data)
        whole_input_list_for_cenral.extend(extract_tx(loaded_data,f))
        offset+=50
    whole_input_list_for_cenral=set(whole_input_list_for_cenral)
    if degree!=max_degree:
        for address in whole_input_list_for_cenral:
            if address==central_address:
                continue
            search_connected(session,address,degree+1,file_dir,max_degree)

def extract_tx(loaded_data,file_handle):
    whole_input_list=[]
    for transaction in loaded_data['txs']:
        #save group of inputs records (address,value)
        #print(transaction)
        flag=0
        input_list=[(input_record["prev_out"]['addr'],input_record["prev_out"]['value']) if 'prev_out'  in input_record and "addr" in input_record['prev_out'] else None for  input_record in transaction["inputs"]]
        #save group of outputs records (address,value)
        output_list=[((output_record['addr'],output_record['value']) if 'addr'  in output_record else None)for  output_record in transaction["out"]]
        transaction_time=transaction['time']
        file_handle.write(str((input_list,output_list,transaction_time))+"\n")
        whole_input_list.extend([input_record["prev_out"]['addr'] if 'prev_out'  in input_record else None  for  input_record in transaction["inputs"]])
    return whole_input_list
