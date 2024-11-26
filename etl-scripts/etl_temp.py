import os
import requests
import mysql.connector
import json
from mysql.connector import Error

class TempCountryETL:
    def __init__(self,conn):
