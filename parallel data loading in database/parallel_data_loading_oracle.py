import cx_Oracle
import os
from datetime import datetime
import csv
import multiprocessing

user = '*****'
password = '*****'
connString = '*****'
directory = "*****"
parallel_process = 20
new_file_list_table_name = "ncdc_new_file_list"
log_table_name = "ncdc_file_log_p" + str(parallel_process)
data_table_name = "ncdc_hourly_data"

def drop_table_if_exists(cur, table_name):
	sql = "select count(*)\n" \
		"from user_objects\n" \
		"where object_type = 'TABLE' \n" \
		"and object_name = upper('" + table_name + "')"
	cur.execute(sql)
	if cur.fetchone()[0] > 0:
		sql = "drop table " + table_name
		cur.execute(sql)

def process_data_and_load(file_list,divider,rem):
	con = cx_Oracle.connect(user=user, password=password, dsn=connString)
    	cur = con.cursor()
    
    	#etl_id = datetime.now().strftime("%Y%m%d%H%M%S")
    	etl_id = str(rem)
	processing_list = []
    	for seq in range(len(file_list)):
        	if seq % divider == rem:
            		processing_list.append(file_list[seq])

    	for fl in processing_list:
        	fn = ''.join(fl)
        	print(fn)
        	record_count = 0
        	loading_start_time = datetime.now().strftime("%Y%m%d%H%M%S")
        	file_name_with_dir = directory + "/" + fn
        	data_set = []
        	with open(file_name_with_dir, 'r') as fo:
            		reader = csv.reader(fo)
            		column_name = next(reader)
            
            		fo.seek(0)
            		countrdr = csv.DictReader(fo)
            		for row in countrdr:
              			record_count += 1

            		fo.seek(0)
            		data_reader = csv.DictReader(fo)
        
            		records = [(i[column_name[0]], i[column_name[1]], i[column_name[2]], i[column_name[3]], 
                		i[column_name[4]], i[column_name[5]], i[column_name[6]], i[column_name[7]], i[column_name[8]], 
                		i[column_name[9]], i[column_name[10]], i[column_name[11]], i[column_name[12]], i[column_name[13]], 
                		i[column_name[14]], i[column_name[15]], i[column_name[16]], fn) 
                		for i in data_reader]
        
        	cur.executemany("insert into " + data_table_name + " (\n" \
            		"   station, dd, source, latitude, longitude, elevation, name, \n" \
            		"   report_type, call_sign, quality_control, wnd, cig, vis, tmp, \n" \
            		"   dew, slp, gf1, file_name \n" \
            		") \n" \
            		"values ( \n" \
            		"    :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18 \n" \
            		")"
        	, records)
              
        	sql = "update " + log_table_name + "\n" \
            		"set etl_id = " + etl_id + ",\n" \
            		"   loading_start_time = to_date('" + loading_start_time + "', 'yyyymmddhh24miss'),\n" \
            		"   loading_end_time = sysdate,\n" \
            		"   record_count = " + str(record_count) + ",\n" \
            		"   loading_status = 'Y'\n" \
            		"where file_name = '" + fn + "'"
        	cur.execute(sql)
        	con.commit()
    
	cur.close()
    	con.close()

if __name__=="__main__":
    
    	con = cx_Oracle.connect(user=user, password=password, dsn=connString)
    	cur = con.cursor()
    
    	file_list = os.listdir(directory)
    
    	drop_table_if_exists(cur, new_file_list_table_name)
    	sql = "create table " + new_file_list_table_name + " (\n" \
        	"   file_name varchar2(50 byte)\n" \
        	")"
    	cur.execute(sql)
    
    	file_list = [(file_name,) for file_name in file_list]
    	cur.executemany("insert into " + new_file_list_table_name + " (file_name) values (:1)", file_list)
    	con.commit()
    
    	drop_table_if_exists(cur, log_table_name)
    	sql = "create table " + log_table_name + " (\n" \
        	"   etl_id NUMBER,\n" \
        	"   file_name varchar2(50 byte),\n" \
        	"   file_insert_time date,\n" \
        	"   loading_start_time date,\n" \
        	"   loading_end_time date,\n" \
        	"   record_count number,\n" \
        	"   loading_status varchar2(1 byte)\n" \
        	")"
    	cur.execute(sql)
    
    	sql = "insert into " + log_table_name + " (file_name, file_insert_time, loading_status)\n" \
        	"select a.file_name, sysdate file_insert_time, 'N' loading_status\n" \
        	"from ( \n" \
        	"   select file_name from " + new_file_list_table_name + "\n" \
        	"   minus\n" \
        	"   select file_name from " + log_table_name + "\n" \
        	") a"
    	cur.execute(sql)
    	con.commit()
    
    	drop_table_if_exists(cur, data_table_name)
    	sql = "create table " + data_table_name + " (\n" \
        	"   station varchar2(50 byte),\n" \
        	"   dd varchar2(50 byte),\n" \
        	"   source varchar2(50 byte),\n" \
        	"   latitude varchar2(50 byte),\n" \
        	"   longitude varchar2(50 byte),\n" \
        	"   elevation varchar2(50 byte),\n" \
        	"   name varchar2(100 byte),\n" \
        	"   report_type varchar2(50 byte),\n" \
        	"   call_sign varchar2(50 byte),\n" \
        	"   quality_control varchar2(50 byte),\n" \
        	"   wnd varchar2(50 byte),\n" \
        	"   cig varchar2(50 byte),\n" \
        	"   vis varchar2(50 byte),\n" \
        	"   tmp varchar2(50 byte),\n" \
        	"   dew varchar2(50 byte),\n" \
        	"   slp varchar2(50 byte),\n" \
        	"   gf1 varchar2(50 byte),\n" \
        	"   file_name varchar2(50 byte)\n" \
       	 	")"
    	cur.execute(sql)
    
    	sql = "select file_name from " + log_table_name + " \n" \
        	"where loading_status = 'N'"
    	cur.execute(sql)
    	unprocessed_file_list = cur.fetchall()
    
    	cur.close()
    	con.close()
    
    	subprocess_list = []
    	for rem in range(parallel_process):
            	proc = multiprocessing.Process(target=process_data_and_load, args=(unprocessed_file_list,parallel_process,rem,))
            	proc.start()
            	subprocess_list.append(proc)

    	for prc in subprocess_list:
            	prc.join()

