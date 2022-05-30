import json
import sys
import pandas as pd

def main():
    df = pd.read_json("./Data/territory.json")
    for i in df:
        print(i[0])
    
if __name__ == '__main__':
    main()
    sys.exit(0)
    