from controller.consultanfe import consultar_nfe, consultar_nfe_sql_server
from controller.db_data import DataBase, DataBaseSQL
from controller.ler_token import buscar_dados_id
import time
from controller.lerconfig import lerconfig
import schedule
from docs.config import *
import datetime


def postgresql():
    db = DataBase()
    _token = db.executa_DQL(sql=f"SELECT TOKEN from FDCONTRATO;")
    dados_api = buscar_dados_id(_token[0][0])
    for i in dados_api:
        logging.info(f"Dados API {i}")
        if 'senha_certificado' in i:
            senha_certificado = i['senha_certificado']
            if not senha_certificado:
                logging.error(
                    f"Senha do certificado não existe na API para a empresa {i['razao_social']} cnpj {i['cnpj']}")
                continue
        if 'cnpj' in i:
            cnpj = i['cnpj']
        if 'data_validade' in i:
            data_validade = i['data_validade']
            if cnpj == '03172011000147':
                data_validade = '2023-06-24 13:48:58'
            data_validade = data_validade[0:10]
            data_validade = time.strptime(data_validade, "%Y-%m-%d")

        data_atual = datetime.datetime.today()
        data_atual = str(data_atual)
        data_atual = data_atual[0:10]
        data_atual = time.strptime(data_atual, "%Y-%m-%d")

        if data_validade < data_atual:
            logging.error(
                f"Certificado vencido para a empresa {i['razao_social']} cnpj {i['cnpj']}")
            continue
        sql_url_certificado = f"""select
                                    ESTADO.UF,
                                    EMPRESA.URL_CERTIFICADO
                                from
                                    PUBLIC.EMPRESA EMPRESA,
                                    PUBLIC.ESTADO ESTADO
                                where EMPRESA.CODIGO_ESTADO = ESTADO.CODIGO_ESTADO
                                    and EMPRESA.CPFCNPJ = '{cnpj}';"""

        retorno_certificado_and_uf = db.executa_DQL(
            sql=sql_url_certificado)

        uf = retorno_certificado_and_uf[0][0]
        if retorno_certificado_and_uf[0][1] == '' or retorno_certificado_and_uf[0][1] == None:
            logging.warning(
                f"Certificado não encontrado para a empresa {i['razao_social']} cnpj {i['cnpj']}")
            time.sleep(5)
            continue
        else:
            certificado = retorno_certificado_and_uf[0][1]
            if certificado[0:1] == '//' or certificado[0:1] == '\\':
                certificado = r'{}'.format(certificado)# r Indica para o python que esse é um diretorio
                print(certificado)

        sql_nsu = f"SELECT NSU from NSULISTA where CODIGO_EMPRESA = (select CODIGO_EMPRESA from EMPRESA where CPFCNPJ= '{cnpj}') ORDER by NSU DESC LIMIT 1;"
        nsu = db.executa_DQL(sql=sql_nsu)
        if nsu == []:
            nsu = 0
        else:
            nsu = nsu[0][0]

        try:
            logging.info(f'Iniciando a consulta para a empresa {cnpj}')
            consulta = consultar_nfe(certificado=certificado, senha=senha_certificado,
                                     uf=uf, homologacao=False, CPFCNPJ=cnpj, NSU=nsu, CHAVE='')
            cStat = consulta[2]

            if cStat == '137':
                print(
                    f'Consulta para a empresa {cnpj} concluida não há mais documentos a pesquisar')
                logging.info(
                    f'Consulta para a empresa {cnpj} concluida não há mais documentos a pesquisar')
            elif cStat == '656':
                print(f'Consulta para a empresa {cnpj} concluida')
                logging.info(
                    f'Consulta para a empresa {cnpj} concluida *CONSUMO INDEVIDO*')
            else:
                print(f'Consulta para a empresa {cnpj} concluida')
                logging.info(f'Consulta para a empresa {cnpj} concluida')
            # time.sleep(1800)
        except (Exception, IndexError) as exe:
            logging.exception(exe)
            continue

def sqlserver():
    db = DataBaseSQL()
    if __name__ == "__main__":

        _token = db.executa_DQL(sql=f"SELECT TOKEN from FDCONTRATO;")
        dados_api = buscar_dados_id(_token[0][0])
        for i in dados_api:
            logging.info(f"Dados API {i}")
            if 'senha_certificado' in i:
                senha_certificado = i['senha_certificado']
                if not senha_certificado:
                    logging.error(
                        f"Senha do certificado não existe na API para a empresa {i['razao_social']} cnpj {i['cnpj']}")
                    continue
            if 'cnpj' in i:
                cnpj = i['cnpj']
            if 'data_validade' in i:
                data_validade = i['data_validade']
                if cnpj == '03172011000147':
                    data_validade = '2023-06-24 13:48:58'
                data_validade = data_validade[0:10]
                data_validade = time.strptime(data_validade, "%Y-%m-%d")

            data_atual = datetime.datetime.today()
            data_atual = str(data_atual)
            data_atual = data_atual[0:10]
            data_atual = time.strptime(data_atual, "%Y-%m-%d")

            if data_validade < data_atual:
                logging.error(
                    f"Certificado vencido para a empresa {i['razao_social']} cnpj {i['cnpj']}")
                continue
            sql_url_certificado = f"""select
                                        ESTADO.UF,
                                        EMPRESA.URL_CERTIFICADO
                                    from
                                            EMPRESA EMPRESA,
                                            ESTADO ESTADO
                                    where EMPRESA.CODIGO_ESTADO = ESTADO.CODIGO_ESTADO
                                        and EMPRESA.CPFCNPJ = '{cnpj}';"""

            retorno_certificado_and_uf = db.executa_DQL(
                sql=sql_url_certificado)
            uf = retorno_certificado_and_uf[0][0]
            if retorno_certificado_and_uf[0][1] == '' or retorno_certificado_and_uf[0][1] == None:
                logging.warning(
                    f"Certificado não encontrado para a empresa {i['razao_social']} cnpj {i['cnpj']}")
                time.sleep(5)
                continue
            else:
                certificado = retorno_certificado_and_uf[0][1]
                if certificado[0:1] == '//' or certificado[0:1] == '\\':
                    certificado = r'{}'.format(certificado)# r Indica para o python que esse é um diretorio
                    print(certificado)

            sql_nsu = f"SELECT TOP (1) NSU from NSULISTA where CODIGO_EMPRESA = (select  CODIGO_EMPRESA from EMPRESA where CPFCNPJ= '{cnpj}') ORDER by NSU DESC ;"
            nsu = db.executa_DQL(sql=sql_nsu)
            if nsu == []:
                nsu = 0
            else:
                nsu = nsu[0][0]

            try:
                logging.info(f'Iniciando a consulta para a empresa {cnpj}')
                consulta = consultar_nfe_sql_server(certificado=certificado, senha=senha_certificado,
                                         uf=uf, homologacao=False, CPFCNPJ=cnpj, NSU=nsu, CHAVE='')
                cStat = consulta[2]

                if cStat == '137':
                    print(
                        f'Consulta para a empresa {cnpj} concluida não há mais documentos a pesquisar')
                    logging.info(
                        f'Consulta para a empresa {cnpj} concluida não há mais documentos a pesquisar')
                elif cStat == '656':
                    print(f'Consulta para a empresa {cnpj} concluida')
                    logging.info(
                        f'Consulta para a empresa {cnpj} concluida *CONSUMO INDEVIDO*')
                else:
                    print(f'Consulta para a empresa {cnpj} concluida')
                    logging.info(f'Consulta para a empresa {cnpj} concluida')
                # time.sleep(1800)
            except (Exception, IndexError) as exe:
                logging.exception(exe)
                continue

config = lerconfig()

tempo_exec = int(config['tempo_exec_consulta'])

if config['tipo_database'] == '1':
    schedule.every(tempo_exec).minutes.do(postgresql)
else:
    schedule.every(tempo_exec).minutes.do(sqlserver)

while True:

    schedule.run_pending()
    time.sleep(1)
