import requests
import spider
import visualization
import api_calls
import os

def mkdir(path):
    folder = os.path.exists(path)

    if not folder:
        os.makedirs(path)
def download_per(address,type_name,file_name,max_degree):
    print(address)
    mkdir(type_name)
    mkdir(type_name+'/'+file_name)
    session=requests.session()
    api_calls.search_connected(session,address,1,type_name+'/'+file_name,max_degree)
#visualization.draw_network("1DkyBEKt5S2GDtv7aQw6rQepAvnsRyHoYM",2,1,"silk_road")