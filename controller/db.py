import sqlite3
from docs.config import *
import psycopg2
from .lerconfig import lerconfig
import pymssql


"""def criartabelas():
    conexao = sqlite3.connect('./ConsultaNotasDestinadas.db')
    sql = [
            "CREATE TABLE IF NOT EXISTS EMPRESA (id INTEGER PRIMARY KEY AUTOINCREMENT, NOME_EMPRESA TEXT, CNPJ VARCHAR(14), URL_CERTIFICADO TEXT, UF_ESTADO CHAR(4));",
            "CREATE TABLE IF NOT EXISTS ESTADO ( CODIGO_ESTADO INTEGER NOT NULL, ESTADO VARCHAR(50) NULL, UF VARCHAR(2) NULL, LIMCONCANCELADA INT4 NULL, CONSTRAINT PK_ESTADO PRIMARY KEY (CODIGO_ESTADO));",
            "CREATE TABLE IF NOT EXISTS USUARIOS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NOME VARCHAR(100) NULL, USUARIO VARCHAR(50) NULL, SENHA VARCHAR(20) NULL);"
           ]

    for create in sql:
            
        cursor = conexao.cursor()
        cursor.execute(create)
        print(f'Tabela criada{create}')
        logging.info(create)"""

class DataBase():
    def __init__(self,db='./ConsultaNotasDestinadas.db'):
        self.db = db

    def conectardb(self):
        self.conexao = sqlite3.connect(database=self.db)
        self.cursor = self.conexao.cursor()

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


config = lerconfig()
class DataBase_2():
    
    
    def __init__(self, host=config['SERVIDOR'], user=config['USUARIO'], port=config['PORTA'],password=config['SENHA_BANCO'], db=config['BANCO']):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.db = db

    def conectardb(self):
            
        self.conexao = psycopg2.connect(host=self.host,
                                        user=self.user,
                                        password=self.password,
                                        database=self.db
                                        )

        self.cursor = self.conexao.cursor()

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