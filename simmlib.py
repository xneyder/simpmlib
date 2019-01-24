#!/usr/bin/python
""" hld2oit.py:

 Description: Tool to simulate data in a PMM library, it creates the connect
    script and executes it subscribing to the given access


 Created by : Daniel Jaramillo
 Creation Date: 11/01/2019
 Modified by:     Date:
 All rights(C) reserved to Teoco
"""
import sys
import os
import shutil
import argparse
import cx_Oracle
import base64
import signal
import subprocess
import time
import glob
import json
import datetime
import pandas as pd
from LoggerInit import LoggerInit
from threading import Thread

class ManagedDbConnection:
    def __init__(self, DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST):
        self.DB_USER = DB_USER
        self.DB_PASSWORD = DB_PASSWORD
        self.ORACLE_SID = ORACLE_SID
        self.DB_HOST = DB_HOST

    def __enter__(self):
        try:
            self.db = cx_Oracle.connect(
                '{DB_USER}/{DB_PASSWORD}@{DB_HOST}/{ORACLE_SID}'\
                .format(
                    DB_USER=self.DB_USER,
                    DB_PASSWORD=self.DB_PASSWORD,
                    DB_HOST=self.DB_HOST,
                    ORACLE_SID=self.ORACLE_SID), threaded=True)
        except cx_Oracle.DatabaseError as e:
            app_logger.error(e)
            quit()
        self.cursor = self.db.cursor()
        sqlplus_script="alter session set nls_date_format = 'DD-MON-YY HH24:MI'"
        try:
            self.cursor.execute(sqlplus_script)
        except cx_Oracle.DatabaseError as e:
            app_logger.error(e)
            app_logger.error(sqlplus_script[0:900])
            quit()
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.db:
            self.db.close()

def run_sqlplus(sqlplus_script):

    """
    Run a sql command or group of commands against
    a database using sqlplus.
    """

    p = subprocess.Popen(['sqlplus','-S','/nolog'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    (stdout,stderr) = p.communicate(sqlplus_script.encode('utf-8'))
    # print(stdout.split("\n"))
    # stdout_lines = stdout.decode('utf-8').split("\n")
    stdout_lines = stdout.split("\n")
    return stdout_lines

def check_running(program,process_name):
    pids=[pid for pid in os.listdir('/proc') if pid.isdigit()]
    pids_found=[]
    for pid in pids:
        if int(pid) == int(os.getpid()):
            continue
        try:
            cmd=open(os.path.join('/proc',pid,'cmdline')).read()
            if process_name in cmd and program in cmd:
                pids_found.append(int(pid))
        except IOError:
            continue
    return pids_found

def kill_process(program,process_name):
    app_logger=logger.get_logger('kill_process')
    pids=check_running(program,process_name)
    if not pids:
        app_logger.error('{process_name} is not running'\
            .format(process_name=process_name))
        return -1                
    for pid in pids:
        os.kill(int(pid), signal.SIGKILL)
    return 0
        


def parse_args():
    """Parse input arguments"""
    app_logger=logger.get_logger("create_access")
    global CONF_FILE
    parser = argparse.ArgumentParser()

    parser.add_argument('-c','--conf',
    	help='Configuration Json file',
    	required=True,
    	type=str)

    args=parser.parse_args()
    CONF_FILE=args.conf
    

def create_access():
    app_logger=logger.get_logger("create_access")
    app_logger.info('Creating {LIBRARY_NAME} GD access'\
        .format(LIBRARY_NAME=LIBRARY_NAME))
    sqlplus_script="""
        connect {DB_USER}/{DB_PASSWORD}@{ORACLE_SID}
        begin
            comm_db.PA_PROJ_MED.SP_INSERT_ACC_L2G(
                IN_SUBNET_NAME=>'{IN_SUBNET_NAME}',
                IN_ACCESS_NAME=>'{IN_ACCESS_NAME}',
                IN_GD_NAME=>'{IN_GD_NAME}',
                IN_LOCAL_DIR=>'{IN_LOCAL_DIR}',
                IN_CYCLE_INTERVAL=>'{IN_CYCLE_INTERVAL}',
                IN_MASK=>'{IN_MASK}',
                IN_ADVAMCED_MASK=>'{IN_ADVAMCED_MASK}',
                IN_AGING_FILTER=>'{IN_AGING_FILTER}',
                IN_SORT_ORDER=>'{IN_SORT_ORDER}',
                IN_SOURCE_FILE_FINISH_POLICY=>'{IN_SOURCE_FILE_FINISH_POLICY}',
                IN_SOURCE_SUFFIX_PREFIX=>'{IN_SOURCE_SUFFIX_PREFIX}',
                IN_LOOK_IN_SUBFOLDERS=>'{IN_LOOK_IN_SUBFOLDERS}',
                IN_SUB_FOLDERS_MASK=>'{IN_SUB_FOLDERS_MASK}',
                IN_POST_SCRIPT=>'{IN_POST_SCRIPT}',
                IN_SHOULD_RETRANSFER=>'{IN_SHOULD_RETRANSFER}',
                IN_RETRANFER_OFFSET=>'{IN_RETRANFER_OFFSET}',
                IN_ENABLEFILEMONITOR=>'{IN_ENABLEFILEMONITOR}',
                IN_NE_NAME=>'{IN_NE_NAME}'
            );
        end;
        /
    """.format(
        DB_USER=DB_USER,
        DB_PASSWORD=DB_PASSWORD,
        ORACLE_SID=ORACLE_SID,
        IN_SUBNET_NAME=LIBRARY_NAME,
        IN_ACCESS_NAME=LIBRARY_NAME,
        IN_GD_NAME=GD_NAME,
        IN_LOCAL_DIR=LOCAL_DIR+"/in_sim/",
        IN_CYCLE_INTERVAL=CYCLE_INTERVAL,
        IN_MASK=MASK,
        IN_ADVAMCED_MASK="",
        IN_AGING_FILTER="",
        IN_SORT_ORDER="NoSort",
        IN_SOURCE_FILE_FINISH_POLICY="Delete",
        IN_SOURCE_SUFFIX_PREFIX="",
        IN_LOOK_IN_SUBFOLDERS="",
        IN_SUB_FOLDERS_MASK="",
        IN_POST_SCRIPT="",
        IN_SHOULD_RETRANSFER="Always",
        IN_RETRANFER_OFFSET="AllFile",
        IN_ENABLEFILEMONITOR=ENABLEFILEMONITOR,
        IN_NE_NAME=NE_NAME
    )
    sqlplus_output = run_sqlplus(sqlplus_script)
    app_logger.info(' '.join(sqlplus_output))
    #Create input and done folders
    if not os.path.exists(LOCAL_DIR+"/in_sim/"):
        os.makedirs(LOCAL_DIR+"/in_sim/")

    with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
        cursor=db.cursor()
        sqlplus_script="""
            select ACCESS_NUM from comm_db.med_access where access_name='{LIBRARY_NAME}'
        """.format(LIBRARY_NAME=LIBRARY_NAME)
        try:
            cursor.execute(sqlplus_script)
            access_id=''
            for row in filter(None,cursor):
                app_logger.info('access id {access_id} was created'\
                    .format(access_id=row[0]))
                access_id=row[0]
        except cx_Oracle.DatabaseError as e:
            app_logger.error(e)
            app_logger.error(sqlplus_script)
            quit()
    app_logger.info('Refreshing {GD_NAME} process'\
        .format(GD_NAME=GD_NAME)) 
    pid=kill_process('GD_Name',GD_NAME)
    if pid !=0:
        app_logger.error('{process_name} is not running'\
            .format(process_name=process_name))
        quit()
    time.sleep(20)
    pid=check_running('GD_Name',GD_NAME)
    if not pid:
        app_logger.error('{process_name} is not running'\
            .format(process_name=process_name))
        quit()
    return access_id

def run_connect():
    """
    Run a library connect script
    """
    app_logger=logger.get_logger("run_connect")
    global connect_log
    global INSTANCE_ID
    global access_id
    #(define GDSubscription "Notification -Protocol File-Transfer -NeTypeName Mediation_Server -AType 10 -Subnet 42756 -NeNum 3150611 -Access 42284")\
    connect_log=os.path.join(TMP_DIR,'{LIBRARY_NAME}.connect.log'\
        .format(LIBRARY_NAME=LIBRARY_NAME))
    args=['connect',
            '-daemon',
            '{LIBRARY_NAME}.connect'.format(LIBRARY_NAME=LIBRARY_NAME),
            '-expr',
            '\'(load "n2_std.connect")\
            (load "n2_logger.connect")\
            (add-log-module "connect" (get-env-else "N2_LOG_DIR" ".")\
                "conductor_AFFIRMED_VMCC_FPP_{INSTANCE_ID}")\
            (export "conductor_AFFIRMED_VMCC_FPP_{INSTANCE_ID}" self)\
            (define conductor-instance-id {INSTANCE_ID})\
            (define library-instance-name "AFFIRMED_VMCC_FPP")\
            (define dvx2-log-location (get-env "DVX2_LOG_DIR") )\
            (define dvx2-log-prefix "dvx2_")\
            (define GDSubscription "Notification -Protocol File-Transfer\
                -Access {access_id}")\
            (define DeactivateAP 0)\
            (define NI_DIR "/tmp")\'\
             > {connect_log} 2>&1'.format(connect_log=connect_log,
                INSTANCE_ID=INSTANCE_ID,
                access_id=access_id)
            ]
    app_logger.info('Running {LIBRARY_NAME}.connect'\
        .format(LIBRARY_NAME=LIBRARY_NAME))
    os.system(' '.join(args))

def delete_data():
    """
    Delete data in target tables for datetime found in raw data files
    """
    app_logger=logger.get_logger("delete_data")
    app_logger.info("Deleting data from target tables")
    for table in table_list:
        for _datetime in datetime_list:
            with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
                cursor=db.cursor()
                sqlplus_script="""
                    delete from {table} 
                    where 
                    datetime = to_date('{_datetime}','YYYY-MM-DD HH24:MI:SS')
                """.format(table=table,
                    _datetime=_datetime
                )
                try:
                    cursor.execute(sqlplus_script)
                    db.commit()
                except cx_Oracle.DatabaseError as e:
                    app_logger.error(e)
                    app_logger.error(sqlplus_script)
                    quit()

    

def parse_dbl():
    """
    Get table list and batchevery time from dbl fiile
    """
    app_logger=logger.get_logger("parse_dbl")
    global table_list
    global batchevery
    global connect_file
    global work_dir_list
    global error_dir_list
    app_logger.info('Parsing {connect_file}'.format(connect_file=connect_file))
    dbl_file=""
    with open(connect_file) as file:
        filedata=file.read().split('\n')
        for line in filedata:
            if ".dbl" in line:
                dbl_file=line.split('"')[1]
                break
    dbl_file=os.path.join(DVX2_IMP_DIR,'config','Dbl',dbl_file)
    app_logger.info('Parsing {dbl_file}'.format(dbl_file=dbl_file))
    with open(dbl_file) as file:
        filedata=file.read().split('\n')
        for line in filedata:
            if "DBProfile" in line:
                profile=line.split("=")[1]
            elif "TargetTable" in line:
                table_list.add(profile+'.'+line.split("=")[1]) 
            elif "BatchEvery" in line:
                batchevery=max(batchevery,line.split("=")[1])
            elif "WorkDir" in line:
                work_dir_list.add(os.path.expandvars(
                    line.split('=')[1].replace('$/','/')))
            elif "ErrorDir" in line:
                error_dir_list.add(os.path.expandvars(
                    line.split('=')[1].replace('$/','/')))
                
def get_tag(file_name,tag):
    """
    resurns the line in the file that contains the tag
    """
    app_logger=logger.get_logger("get_tag")
    result=""
    with open(file_name,'r') as file:
        filedata=file.read().split("\n")
        for line in filedata:
            if tag in line:
                result=line
                break
    return result

def get_column(file_name,column):
    """
    resurns the vallues in the file for the given column
    """
    app_logger=logger.get_logger("get_column")
    #clean up the file
    data=[]
    with open(file_name,'r') as file:
        filedata=file.read().split('\n')
        for line in filedata:
            if configuration['post_tag_string'] in line:
                continue
            data.append(line)

    tmp_file_name=file_name+".tmp"
    with open(tmp_file_name,'w') as file:
        for line in data:
            file.write(line+'\n')

    df=pd.read_csv(tmp_file_name,sep=configuration['delimiter'])
    os.remove(tmp_file_name)    
    return list(df.loc[:,column])


def get_keys():
    """
    Get table list and batchevery time from dbl fiile
    """
    global datetime_list
    global ne_list
    app_logger=logger.get_logger("get_keys")
    global configuration
    NE_NAME=configuration['NE_NAME']
    rd_file_list=glob.glob(os.path.join(LOCAL_DIR,MASK))
    #get datetime
    for file_name in rd_file_list:
        function_list=[]
        if configuration['DATETIME']['source'].lower()=="filename":
            bfile_name=os.path.basename(file_name)
            function_list.append(\
                configuration['DATETIME']['function']\
                    .replace('input',"'"+bfile_name+"'")
            )

        elif configuration['DATETIME']['source'].lower()=="tag":
            line=get_tag(file_name,configuration['DATETIME']['tag'])
            function_list.append(
                configuration['DATETIME']['function']\
                    .replace('input',"'"+line+"'")
            )

        elif configuration['DATETIME']['source'].lower()=="column":
            tmp_list=get_column(file_name,
                configuration['DATETIME']['column'])            
            function_list=[]
            for line in tmp_list:
                function_list.append(configuration['DATETIME']['function']\
                    .replace('input',"'"+str(line)+"'")) 
        else:
            app_logger.error('Wrong DATETIME configuration {DATETIME}'\
                .format(DATETIME=DATETIME)) 
            quit()
                
        if not function_list:
            app_logger.error('DATETIME not found in file {file_name} \
                configuration {conf}'.format(conf=configuration['DATETIME'],
                file_name=file_name))
            quit()

        for function in function_list:
            try:
                datetime_str=eval(function)
            except Exception as e:
                app_logger.error(e)
                quit()
            datetime_list.add(datetime.datetime.strptime(datetime_str,
                configuration['DATETIME']['format']))

def copy_rd():
    """
    Copy raw data files to the input folder 
    """
    global LOCAL_DIR
    global MASK
    app_logger=logger.get_logger("copy_rd")
    target_dir=os.path.join(LOCAL_DIR,'in_sim')
    app_logger.info('Copying rd files to {target_dir}'\
        .format(target_dir=target_dir))
    rd_file_list=glob.glob(os.path.join(LOCAL_DIR,MASK))
    for file in rd_file_list:
        shutil.copy(file,target_dir)

def wait_rd():
    """
    Wait for raw data to be processed
    """
    global LOCAL_DIR
    global MASK
    app_logger=logger.get_logger("wait_rd")
    target_dir=os.path.join(LOCAL_DIR,'in_sim')
    while True:
        rd_files=glob.glob(os.path.join(target_dir,MASK))
        if len(rd_files) == 0:
            break
        app_logger.info('{rd_files} raw data files on queue'\
            .format(rd_files=len(rd_files)))
        time.sleep(10)
    time.sleep(30)

def wait_bcp():
    """
    Wait for bcp files to be processed
    """
    global work_dir_list
    global INSTANCE_ID
    app_logger=logger.get_logger("wait_bcp")
    while True:
        bcp_files=[]
        for dir in work_dir_list:
            bcp_files.extend(glob.glob(dir+"/*{INSTANCE_ID}*"\
                .format(INSTANCE_ID=INSTANCE_ID)))
        if len(bcp_files) == 0:
            break
        app_logger.info('{bcp_files} bcp files on queue'\
            .format(bcp_files=len(bcp_files)))
        time.sleep(10)

def wait_connect():
    """
    Wait for connect to come up
    """
    global DVX2_LOG_FILE
    app_logger=logger.get_logger("wait_connect")
    do_loop=True
    while do_loop:
        app_logger.info("Waiting for connect to come up")
        time.sleep(10)
        with open(DVX2_LOG_FILE) as file:
            filedata=file.read().split('\n')
            for line in filedata:
                if "Fatal error" in line:
                    app_logger.error(line)
                    quit()
                elif "Subcribed to" in line:
                    do_loop=False
                    break

def main():
    app_logger=logger.get_logger("main")
    global DVX2_IMP_DIR
    global DVX2_LOG_DIR
    global DVX2_LOG_FILE
    global connect_file
    global access_id
    global LIBRARY_NAME
    global MASK
    global LOCAL_DIR
    global configuration
    parse_args()

    try:
        with open(CONF_FILE) as json_file:
            configuration=json.load(json_file)
    except IOError as e:
        app_logger_local.error(e)
        quit()

    LIBRARY_NAME=configuration['library']
    MASK=configuration['mask']
    LOCAL_DIR=configuration['input_rd_path']

    #Validate environment variables
    if 'DVX2_IMP_DIR' not in os.environ:
        app_logger.error('DVX2_IMP_DIR env variable not defined') 
        quit()
    DVX2_IMP_DIR=os.environ['DVX2_IMP_DIR']
    if 'DVX2_LOG_DIR' not in os.environ:
        app_logger.error('DVX2_LOG_DIR env variable not defined') 
        quit()
    DVX2_LOG_DIR=os.environ['DVX2_LOG_DIR']
    DVX2_LOG_FILE=os.path.join(DVX2_LOG_DIR,\
        "dvx2_{LIBRARY_NAME}_{INSTANCE_ID}.log"\
        .format(LIBRARY_NAME=LIBRARY_NAME,INSTANCE_ID=INSTANCE_ID))
    #Make log file empty
    open(DVX2_LOG_FILE, 'w').close()
    #Validate if Library exists
    connect_file=os.path.join(DVX2_IMP_DIR,'scripts',LIBRARY_NAME+'.connect')
    if not connect_file:
        app_logger.error('Library {LIBRARY_NAME} does not exist'\
            .format(LIBRARY_NAME=LIBRARY_NAME)) 
        quit()
    #Validate raw data files
    if not os.path.isdir(LOCAL_DIR):
        app_logger.error('Input dir {LOCAL_DIR} does not exist'\
            .format(LOCAL_DIR=LOCAL_DIR)) 
        quit()
    if len(glob.glob(os.path.join(LOCAL_DIR,MASK))) ==0:
        app_logger.error('No raw data files available in {LOCAL_DIR}'\
            .format(LOCAL_DIR=LOCAL_DIR)) 
        quit()


    #Create GD Access
    access_id=create_access()
    if not access_id:
        app_logger.error('Access could not be created')
        quit()

    #Parse DBL file
    parse_dbl()

    #Get all keys in the raw data
    get_keys()

    #Delete the dat ain the tables
    delete_data()

    #Run connect
    worker = Thread(target=run_connect, args=())
    worker.setDaemon(True)
    worker.start()
    wait_connect()

    #Copy rd files to input folder
    copy_rd()
       
    #Wait for raw data to be processed
    wait_rd()

    #Wait for bcp files to be processed
    wait_bcp()

    #Kill connect
    app_logger.info('Stopping connect file')
    kill_process('connect','{LIBRARY_NAME}_{INSTANCE_ID}'\
        .format(LIBRARY_NAME=LIBRARY_NAME,
            INSTANCE_ID=INSTANCE_ID))

    #Delete data from tables tables
    #delete_data()


if __name__ == "__main__":
    #constants
    TMP_DIR=os.path.join(os.getcwd(),'tmp')
    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)
    DB_USER=os.environ['DB_USER']
    DB_PASSWORD=base64.b64decode(os.environ['DB_PASSWORD'])
    ORACLE_SID=os.environ['ORACLE_SID']
    DB_HOST=os.environ['DB_HOST']
    #If LOG_DIR environment var is not defined use /tmp as logdir
    if 'LOG_DIR' in os.environ:
        log_dir=os.environ['LOG_DIR']
    else:
        log_dir="/tmp"

    log_file=os.path.join(log_dir,"{script}.log".format(script=sys.argv[0])) 
    LIBRARY_NAME=''
    GD_NAME='GD_MEDIATION'
    LOCAL_DIR=''
    CYCLE_INTERVAL='10'
    MASK='*'
    SOURCE_FILE_FINISH_POLICY='Move'
    ENABLEFILEMONITOR="0"
    NE_NAME='Mediation_Server'
    DVX2_IMP_DIR=''
    DVX2_LOG_DIR=''
    DVX2_LOG_FILE=''
    INSTANCE_ID="1717"
    CONF_FILE=""
    configuration={}
    connect_log=""
    connect_file=""
    table_list=set()
    work_dir_list=set()
    error_dir_list=set()
    datetime_list=set()
    ne_list=set()
    batchevery=30
    access_id=""
    logger=LoggerInit(log_file,10)
    main()
