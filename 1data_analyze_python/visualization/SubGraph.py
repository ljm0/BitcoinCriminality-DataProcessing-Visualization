import networkx as nx
# import os
# my_file = 'gpickle_subgraph.gpickle'
# if os.path.exists(my_file):
#     os.remove(my_file)
#     #os.unlink(my_file)
# else:
#     print('no file:%s' % my_file)
pathA = 'E:/BitCoin/data/structures/'
filenameA = 'donator2_sub_14ELQwuU9JXxcuCUi7eQmKs8eTrpLZTnGR.gpickle'
CenterAddressA = '14ELQwuU9JXxcuCUi7eQmKs8eTrpLZTnGR'

DicCrime = {'18firbfmx4KoNeM4cBhcDdXgp2Aiduo43G': 'Sextortion_Sex06.gpickle',
            '18QGMXBte2fVodcq9xCwvPWiBEd98LwHwS': 'Sextortion_Sex10.gpickle', '12DA8mpQCnTB1cEHLPFU7ckP44zN5Xmgu3': 'Sextortion_Sex12.gpickle'}
# {'15LKRRYM2k2CCSGT76rNbQmciLZJSxKXAx': 'Porn_Input1.gpickle', '18aVwkFAadCvwGBHN8vagouWBWrNEpZAaV': 'Porn_Input2.gpickle', '1DzM9y4fRgWqpZZCsvf5Rx4HupbE5Q5r4y': 'Sextortion_Sex01.gpickle', '1DuDhqSWdmRxJjaRRSpa9wRH7yf9ncgw56': 'Sextortion_Sex03.gpickle', '16acVRG2RdMDSmdVuve1N1bYBFu8Rr3iii': 'Sextortion_Sex08.gpickle', '13H6Nszz1xP6Xn5tDKAUVWpm97vLfcNMVi': 'Sextortion_Sex13.gpickle', '19i2Zn4QMg5y9gDTRfLyPXaw2zY3ikPzqg': 'Sextortion_Sex14.gpickle', '144CDUeBhcwoEUmA2B1cL5p5PqZrhJWCCt': 'Sextortion_Sex15.gpickle','1LEJuBuc8bt6quD3TCGeX8o1EQ29qLErCc': 'Sextortion_Sex16.gpickle', '1Pau7aAFNZ5rvogxMnfP37zDAzoeipCqPP': 'Sextortion_Sex17.gpickle', '14dEvzyftZjrTjXaX5XXHo65C1rdsqCw1s': 'Sextortion_Sex18.gpickle', '1CA74qzKno94zf3xFNvQ8TKR5sBpc6bimm': 'Sextortion_Sex19.gpickle', '1PeMj6hcYMCu1eKUAjRq4t7N6EHwDbGyBR': 'ExtortionEmail_Extortion03.gpickle', '19kQT8RpsNNAM5UQ6RjRxRcHbeHhYf6SES': 'ExtortionEmail_Extortion05.gpickle', '1LEBKnvLEZk2cUfyJ1vU5FpueakWyh4m28': 'ExtortionEmail_Extortion06.gpickle', '1NLh41sRP8QfSkkBrps1wXQXQhyM86HQYh': 'ExtortionEmail_Extortion08.gpickle'}

DicNormal = {'1Eativ8CxgM6XQoQZbtEgH9GZzyy4qk5Q8': 'donator2_950289B0D0792052E9B5E95D55884EFAABA0B186.gpickle', '1GTf86w6U5Dp1LZdcs99JNj5ctmqrqnxp4': 'donator2_A994865847A6232304AB2DC934BB116FF79A22EB.gpickle', '1HnbueuGTUd42D8TrzD9bUCxvquTRPgD33': 'donator2_B821DED28E0CBE1D809244FFBBBB9611CA51FA4E.gpickle', '1AV8b35YrpDjZQ2VbbbV4Vf4odFD7rRmfP': 'donator2_680B1CB5A1590662F66823B866E8035483ACCFDE.gpickle', '1JRz8EmDEENBb821caqiuAdvwRBnA3ACqY': 'donator2_BF33CE88A74DF6AB0424938F3EFDD6715C96FA00.gpickle', '1AmrTbctN5d8SF3bRowFacjWAvx1VK2uHq': 'donator2_6B34B865EB075648D21663B7A475F2A7EB090C5B.gpickle','16QrWgSBTwFoaMRi3tbdYEXsxhBA8eAktw': 'donator2_3B5B86BE83B057E59DB5F4B303CD483EA74197A4.gpickle'}

 # {'1Eativ8CxgM6XQoQZbtEgH9GZzyy4qk5Q8': 'donator2_950289B0D0792052E9B5E95D55884EFAABA0B186.gpickle', '1GTf86w6U5Dp1LZdcs99JNj5ctmqrqnxp4': 'donator2_A994865847A6232304AB2DC934BB116FF79A22EB.gpickle', '1HnbueuGTUd42D8TrzD9bUCxvquTRPgD33': 'donator2_B821DED28E0CBE1D809244FFBBBB9611CA51FA4E.gpickle', '14ELQwuU9JXxcuCUi7eQmKs8eTrpLZTnGR': 'donator2_236DFD6A6794B1B9E8FA771397198BE196C630BB.gpickle', '153b1uipoiursFUwH4ysqKJHrecXKipwPH': 'donator2_2C5DBADB80AA0B2227193CE588ED91599069E030.gpickle', '16QrWgSBTwFoaMRi3tbdYEXsxhBA8eAktw': 'donator2_3B5B86BE83B057E59DB5F4B303CD483EA74197A4.gpickle', '18xstBzJSjBGzNrc2ri2hSJ2qPKKEiMwLV': 'donator2_575A3BA7413C20EC58AF67EFD287525421091F35.gpickle', '18zUkf7W5zY7ZdQg7Cda8cSW2XSaagcBDT':
            #  'donator2_57A7C1D18064A28C6CD4AC5780EDA525EA4EA458.gpickle', '1AV8b35YrpDjZQ2VbbbV4Vf4odFD7rRmfP': 'donator2_680B1CB5A1590662F66823B866E8035483ACCFDE.gpickle', '1AkF8Fr9qhxLtRz6XK5T2cFcnN7okXKcv6': 'donator2_6AE6CF3855FEAC7A0C02B371DEF7AD5FBED66CA4.gpickle', '1AmrTbctN5d8SF3bRowFacjWAvx1VK2uHq': 'donator2_6B34B865EB075648D21663B7A475F2A7EB090C5B.gpickle', '1Aw7qFziLu2giXNrMuRJvasZ21if1XF7UP': 'donator2_6CF54C0E51E363D77F6322009A3563EB13659993.gpickle', '1JRz8EmDEENBb821caqiuAdvwRBnA3ACqY': 'donator2_BF33CE88A74DF6AB0424938F3EFDD6715C96FA00.gpickle', '1MQMf6YUqzpADJBpMs4QZrCYrVRhfzvmY4': 'donator2_DFCD5384941826327578E362F68601CB3F6F681E.gpickle', '1MSHDfCGiTJD4M9TyX1mRPJbUTTVp9k677': 'donator2_E02A73FA08136D967839E9435EED7A4BAB6DB922.gpickle'}

pathB = 'E:/BitCoin/data/donor/'

def subgraph(path, filename, CenterAddress, k=3):
    #find out the subgraph which subpoints no less than k交易笔数大于等于3笔的节点
    G = nx.DiGraph(directed=True)
    G = nx.read_gpickle(path + filename)
    G.remove_edges_from(nx.selfloop_edges(G))
    G2 = nx.k_core(G,k)
    return G2

# nx.write_gpickle(subgraph(pathA, filenameA, CenterAddressA, 3), "Sub_{}".format(Cvalue))

# for Ckey, Cvalue in DicCrime.items():
#     nx.write_gpickle(subgraph(pathA, Cvalue, Ckey, 3), "Sub_{}".format(Cvalue))

for Nkey, Nvalue in DicNormal.items():
    nx.write_gpickle(subgraph(pathB, Nvalue, Nkey, 3), "Sub_{}".format(Nvalue))
