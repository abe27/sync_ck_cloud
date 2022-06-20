uvicorn api:app --reload --host 0.0.0.0 --port 5000 --workers 4
uvicorn fifo:app --reload --host 0.0.0.0 --port 5050 --workers 4
/home/seiwa/anaconda3/bin/uvicorn api:app --reload --host 0.0.0.0 --port 8081
/home/seiwa/service/sync_ck_cloud
C:/tools/Anaconda3/Scripts/activate
sudo systemctl stop serial.trigger.service&& sudo systemctl start serial.trigger.service

### SELECT SHELVE,PARTNO,RUNNINGNO,LOTNO,RECLOCATE,PALLETKEY,UPDDTE FROM TXP_CARTONDETAILS WHERE SHELVE='SNON' ORDER BY SHELVE,PARTNO,RUNNINGNO,LOTNO,UPDDTE
### UPDATE TXP_CARTONDETAILS SET SIDTE=NULL,SINO=NULL,SIID=NULL,STOCKQUANTITY=RECEIVINGQUANTITY,SHELVE='SNON'  WHERE RUNNINGNO='S2F0338297'
### UPDATE TXP_CARTONDETAILS SET RECLOCATE=SHELVE,SIDTE=sysdate,SINO='TIMVOUT',SIID='SKTSYS',STOCKQUANTITY=0,SHELVE='S-PLOUT'  WHERE RUNNINGNO='S2F0338297'

### update tbt_order_plans set is_generated=false;
### truncate table tbt_orders restart identity cascade;
### update tbt_consignees set last_running_no=0;
### update tbt_order_details set set_pallet_ctn=0;


select id,file_gedi_id,etdtap,shiptype,pono,partno,balqty,bistdp,balqty/bistdp ctn,reasoncd,created_at tap_date,updated_at spl_sync from tbt_order_plans where pono='#TP2074C' and partno='7198-4980-30' order by created_at,updated_at 