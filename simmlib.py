#!/usr/bin/python
""" hld2oit.py:

 Description: Tool to simulate data in a PMM library, it creates the connect
    script and executes it subscribing to the given access


 Created by : Daniel Jaramillo
 Creation Date: 11/01/2019
 Modified by:     Date:
 All rights(C) reserved to Teoco
"""
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-a','--access',
	help='GD acess number',
	required=True,
	type=str)

parser.add_argument('-l','--lib',
	help='Library name',
	required=True,
	type=str)

parser.add_argument('-t','--dbl_time',
	help='Dbl batchevery timeout in seconds',
	type=str)

args = parser.parse_args()


