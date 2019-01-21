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
import argparse
import cx_Oracle
import base64
import signal
import subprocess
import time
import glob
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
    for pid in pids:
        if int(pid) == int(os.getpid()):
            continue
        try:
            cmd=open(os.path.join('/proc',pid,'cmdline')).read()
            if process_name in cmd and program in cmd:
                return(int(pid))
        except IOError:
            continue
    return None

def kill_process(program,process_name):
    app_logger=logger.get_logger('kill_process')
    pid=check_running(program,process_name)
    if not pid:
        app_logger.error('{process_name} is not running'\
            .format(process_name=process_name))
        quit()
    os.kill(int(pid), signal.SIGKILL)
    time.sleep(10)
    pid=check_running(program,process_name)
    if not pid:
        app_logger.error('{process_name} is not running'\
            .format(process_name=process_name))
        quit()
        


def parse_args():
    """Parse input arguments"""
    app_logger=logger.get_logger("create_access")
    global LOCAL_DIR
    global LIBRARY_NAME
    global MASK
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input_dir',
    	help='Raw data input dir',
    	required=True,
    	type=str)

    parser.add_argument('-l','--lib',
    	help='Library name',
    	required=True,
    	type=str)

    parser.add_argument('-m','--mask',
    	help='File mask for the GD access',
    	type=str)

    args=parser.parse_args()
    LIBRARY_NAME=args.lib
    LOCAL_DIR=args.input_dir
    if args.mask:
        MASK=args.mask
    

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
        IN_SORT_ORDER="",
        IN_SOURCE_FILE_FINISH_POLICY=LOCAL_DIR+"/done_sim/",
        IN_SOURCE_SUFFIX_PREFIX="",
        IN_LOOK_IN_SUBFOLDERS="",
        IN_SUB_FOLDERS_MASK="",
        IN_POST_SCRIPT="",
        IN_SHOULD_RETRANSFER="",
        IN_RETRANFER_OFFSET="",
        IN_ENABLEFILEMONITOR=ENABLEFILEMONITOR,
        IN_NE_NAME=NE_NAME
    )
    sqlplus_output = run_sqlplus(sqlplus_script)
    app_logger.info(' '.join(sqlplus_output))

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
    #kill_process('GD_Name',GD_NAME)
    return access_id

def run_connect():
    """
    Run a library connect script
    """
    app_logger=logger.get_logger("run_connect")
    tmp_file=os.path.join(TMP_DIR,'{LIBRARY_NAME}.connect.log'\
        .format(LIBRARY_NAME=LIBRARY_NAME))
    args=['connect',
            '-daemon',
            '{LIBRARY_NAME}.connect'.format(LIBRARY_NAME=LIBRARY_NAME),
            '-expr',
            '\'(load "n2_std.connect") (load "n2_logger.connect") (add-log-module "connect" (get-env-else "N2_LOG_DIR" ".") "conductor_AFFIRMED_VMCC_FPP_1717") (export "conductor_AFFIRMED_VMCC_FPP_1717" self) (define conductor-instance-id 1717)(define library-instance-name "AFFIRMED_VMCC_FPP")(define dvx2-log-location (get-env "DVX2_LOG_DIR") ) (define dvx2-log-prefix "dvx2_") (define GDSubscription "Notification -Protocol File-Transfer -NeTypeName Mediation_Server -AType 10 -Subnet 42756 -NeNum 3150611 -Access 42284") (define DeactivateAP 0)(define NI_DIR "/teoco/sa_root_med01/implementation/DVX2/data/NI")\''
            ' > {tmp_file} 2>&1'.format(tmp_file=tmp_file)
            ]
    app_logger.info('Running {LIBRARY_NAME}.connect'\
        .format(LIBRARY_NAME=LIBRARY_NAME))
    os.system(' '.join(args))

def delete_data():
    """
    Delet data in target tables for datetime found in raw data files
    """
    app_logger=logger.get_logger("delete_data")

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
                work_dir_list.add(line.split("=")[1]) 
            elif "ErrorDir" in line:
                error_dir_list.add(line.split("=")[1]) 
                
    

def get_datetime():
    """
    Get table list and batchevery time from dbl fiile
    """
    global datetime_list
    app_logger=logger.get_logger("get_datetime")


def main():
    app_logger=logger.get_logger("main")
    global DVX2_IMP_DIR
    global DVX2_LOG_DIR
    global connect_file
    parse_args()

    #Validate environment variables
    if 'DVX2_IMP_DIR' not in os.environ:
        app_logger.error('DVX2_IMP_DIR env variable not defined') 
        quit()
    DVX2_IMP_DIR=os.environ['DVX2_IMP_DIR']
    if 'DVX2_LOG_DIR' not in os.environ:
        app_logger.error('DVX2_LOG_DIR env variable not defined') 
        quit()
    DVX2_LOG_DIR=os.environ['DVX2_LOG_DIR']
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
    quit()

    #Parse DBL file
    parse_dbl()

    #Get datetime list
    get_datetime()

    quit()
    #Run connect
    worker = Thread(target=run_connect, args=())
    worker.setDaemon(True)
    worker.start()
    while True:
        print('Main')
        time.sleep(5)

    
    #Delete data from tables tables
    delete_data()


if __name__ == "__main__":
    print os.getcwd()
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
    INSTANCE_ID="1717"
    connect_log=""
    connect_file=""
    table_list=set()
    work_dir_list=set()
    error_dir_list=set()
    datetime_list=set()
    batchevery=30
    logger=LoggerInit(log_file,10)
    main()
