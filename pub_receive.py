import time
import zmq
import uuid
import datetime
import json
import os
from dotenv import load_dotenv
load_dotenv()

def main():
    """main method"""

    # Prepare our context and publisher
    context = zmq.Context()
    publisher = context.socket(zmq.PUB)
    publisher.bind(f"tcp://*:5050")

    while True:
        inv_no = str("TI2022051401").encode()
        plan_ctn = str("1000").encode()
        rec_ctn = str("20").encode()
        publisher.send_multipart([inv_no, plan_ctn, rec_ctn])
        

    # # We never get here but clean up anyhow
    publisher.close()
    context.term()


if __name__ == "__main__":
    main()