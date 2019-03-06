import load2network
import os
for tag in os.listdir("Ransomware"):
    #print(tag)
    load2network.save_structure("Ransomware",tag)