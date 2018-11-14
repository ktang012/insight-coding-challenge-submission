import os
import sys
import json

import TransactionGraph as TG

def extract_data(json_line):
    parsed = json.loads(json_line)
    if len(parsed) == 3 and set(("created_time", "target", "actor")).issuperset(parsed.keys()):
        return parsed
    else:
        return None

def main():
    tg = TG.TransactionGraph(window_sz=60)
    with open(sys.argv[1], 'r') as infile, open(sys.argv[2], 'w') as outfile:
        for line in infile:
            data = extract_data(line)
            if data == None:
                outfile.write('None\n')
            else:
                median = tg.get_median(data)
                print("{0:.2f}".format(median))
                outfile.write("{0:.2f}".format(median) + '\n')

if __name__ == '__main__':
    if len(sys.argv) == 3:
        main()
    else:
        print("Incorrect usage: requires input and output files")
        
        
    
