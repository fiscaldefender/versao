from docs.config import *
import psycopg2
from .lerconfig import lerconfig


config = lerconfig()


class DataBase():

    def __init__(self, host=config['host'], user=config['user'], port=config['port'], password=config['password'], db=config['database']):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.db = db

    def conectardb(self):
        try:

            self.conexao = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.db
                                            )

            self.cursor = self.conexao.cursor()
        except Exception as erro:
            logging.exception(erro)

    def desconecta(self):
        self.conexao.close()

    def executa_DQL(self, sql):
        self.conectardb()
        self.cursor.execute(sql)
        resposta = self.cursor.fetchall()
        self.desconecta()
        return resposta

    def executa_DML(self, sql):
        self.conectardb()
        self.cursor.execute(sql)
        self.conexao.commit()
        self.desconecta()

    def conexao_ao_db(self):
        conexao = psycopg2.connect(host=self.host,
                                            user=self.user,
                                            password=self.password,
                                            database=self.db
                                            )

        return conexao     

import pymssql

class DataBaseSQL():
    
    
    def __init__(self, host=config['host'], user=config['user'], port=config['port'],password=config['password'], db=config['database']):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.db = db
        


    def conectardb(self):
        try:
                    
            self.conexao = pymssql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.db
                                            )

            self.cursor = self.conexao.cursor()
        except Exception as erro:
            logging.exception(erro)
            
    def desconecta(self):
        self.conexao.close()

    def executa_DQL(self, sql):
        self.conectardb()
        self.cursor.execute(sql)
        resposta = self.cursor.fetchall()
        self.desconecta()
        return resposta

    def executa_DML(self, sql):
        self.conectardb()
        self.cursor.execute(sql)
        self.conexao.commit()
        self.desconecta()
    
    def conexao_ao_db(self):
        conexao = psycopg2.connect(host=self.host,
                                            user=self.user,
                                            password=self.password,
                                            database=self.db
                                            )

        return conexao