uvicorn api:app --reload --host 0.0.0.0 --port 5000 --workers 4
/home/seiwa/anaconda3/bin/uvicorn api:app --reload --host 0.0.0.0 --port 8081
/home/seiwa/service/sync_ck_cloud
C:/tools/Anaconda3/Scripts/activate
sudo systemctl stop serial.trigger.service&& sudo systemctl start serial.trigger.service

### SELECT SHELVE,PARTNO,RUNNINGNO,LOTNO,RECLOCATE,PALLETKEY,UPDDTE FROM TXP_CARTONDETAILS WHERE SHELVE='SNON' ORDER BY SHELVE,PARTNO,RUNNINGNO,LOTNO,UPDDTE
### UPDATE TXP_CARTONDETAILS SET MFGDTE=NULL,SIDTE=NULL,SINO=NULL,SIID=NULL,STOCKQUANTITY=RECEIVINGQUANTITY  WHERE RUNNINGNO='S2F0338297'
### UPDATE TXP_CARTONDETAILS SET RECLOCATE='SNON',MFGDTE=sysdate,SIDTE=sysdate,SINO='TIMVOUT',SIID='SKTSYS',STOCKQUANTITY=0  WHERE RUNNINGNO='S2F0338297'

### update tbt_order_plans set is_generated=false;
### truncate table tbt_orders restart identity cascade;
### update tbt_consignees set last_running_no=0;