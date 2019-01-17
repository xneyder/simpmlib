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
from LoggerInit import LoggerInit

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


def kill_process(program,process_name):
    pids=[pid for pid in os.listdir('/proc') if pid.isdigit()]
    for pid in pids:
        if int(pid) == int(os.getpid()):
            continue
        try:
            cmd=open(os.path.join('/proc',pid,'cmdline')).read()
            if process_name in cmd and program in cmd:
                os.kill(int(pid), signal.SIGKILL)
                return(int(pid))
        except IOError:
            continue
        


def parse_args():
    """Parse input arguments"""
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

    parser.add_argument('-t','--dbl_time',
    	help='Dbl batchevery timeout in seconds',
    	type=str)

    parser.add_argument('-m','--mask',
    	help='File mask for the GD access',
    	type=str)

    args=parser.parse_args()
    LOCAL_DIR=args.input_dir
    LIBRARY_NAME=args.lib
    if args.mask:
        MASK=args.mask
    

def create_access():
    app_logger=logger.get_logger("create_access")
    """with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
        cursor=db.cursor()
        try:
            app_logger.info('Creating {LIBRARY_NAME} GD access'\
                .format(LIBRARY_NAME=LIBRARY_NAME))
            cursor.callproc("comm_db.PA_PROJ_MED.SP_INSERT_ACC_L2G", 
                keywordParameters = dict(
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
                IN_NE_NAME=NE_NAME)
            )
        except cx_Oracle.DatabaseError as e:
            app_logger.error(e)
            app_logger.error("exec comm_db.PA_PROJ_MED.SP_INSERT_ACC_L2G")
            quit()"""
    
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
    pid=kill_process('GD_Name',GD_NAME)
    if not pid:
        app_logger.error('GD {GD_NAME} is not running'.format(GD_NAME=GD_NAME))
        quit()
    return access_id

def main():
    app_logger=logger.get_logger("main")
    parse_args()
    access_id=create_access()
    if not access_id:
        app_logger.error('Access could not be created')
        quit()



if __name__ == "__main__":
    #constants
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
    logger=LoggerInit(log_file,10)
    main()
