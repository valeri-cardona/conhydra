import snap7
import time
import struct
import numpy
import csv
from datetime import datetime
import mariadb
import sys

class communication_s7:
    def __init__(self, ip, rack, slot, db, offset, words):
        self.ip = ip
        self.rack = rack
        self.slot = slot
        self.db = db
        self.offset = offset
        self.words = words

    def get_time_stamp(self):
        now = datetime.now()
        time_stamp = datetime.timestamp(now)
        return datetime.fromtimestamp(time_stamp)    
        #return now    

    def log(self, mensaje):
        with open('log.csv', 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([self.get_time_stamp(), mensaje])
            file.close()     

    def register_data(self, data):
        row=tuple([self.get_time_stamp()] + data)  
        try:
            conn = mariadb.connect(
                user="elicogrp",
                password="Elico2022",
                host="datosdummies.crtpfqqqsc7r.us-east-1.rds.amazonaws.com",
                port=3306,
                database="data" #se habilita cuando se cree una base de datos, para conectarse con la instancia se deshabilita

            )

        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)

        cur = conn.cursor()
        
        cantidad=','.join('?'*11)
        query=f'''
        INSERT INTO t2 VALUES (
            {cantidad}
        );
        '''
        cur.execute(query, row)
        conn.commit()
        conn.close()

    def test_connection(self):
        return self.client.get_cpu_state() != 'S7CpuStatusUnknown'

    def connect(self):
        self.log(f'Inicio de conexion con el PLC de direccion IP: {self.ip}')
        self.client = snap7.client.Client()
        try:
            self.client.connect(self.ip, self.rack, self.slot)
        except RuntimeError:
            self.log('Conexion fallida')
        time.sleep(1)
        if self.test_connection():
            self.log('Conexion exitosa')

    def reconnect(self):
        while self.client.get_cpu_state() == 'S7CpuStatusUnknown':
            time.sleep(5)
            self.log('Reconexion')
            self.connect()

    def read_data(self):
        index='>'+'H'*self.words
        data = self.client.db_read(self.db, self.offset, self.words*2)
        resultado = struct.unpack(index, data)
        return list(resultado)

    def sesion(self):
        self.connect()
        while True:
            if self.client.get_cpu_state() == 'S7CpuStatusUnknown':         
                self.reconnect() 
            else:
                self.register_data(self.read_data())
                time.sleep(60)

plc = communication_s7('192.168.0.1', 0, 0, 5, 0, 10)
plc.sesion()