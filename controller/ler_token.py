from datetime import datetime
import requests
import json
from docs.config import *

def buscar_dados_id(token):

    request = requests.get(f"http://sistema.fiscaldefender.com.br/api/v1/contrato/{token}")
    todo = json.loads(request.content)
    empresas = todo['contrato']['empresas']
    contrato = todo['contrato']['data_fim']

    data_validade = contrato[0:10]
    data_validade = str(data_validade)
    data_validade = datetime.strptime(contrato, "%d/%m/%Y")
    data_validade = data_validade.strftime("%Y-%m-%d")
    data_validade = datetime.strptime(data_validade, "%Y-%m-%d")

    data_atual = datetime.today()
    data_atual = str(data_atual)
    data_atual = data_atual[0:10]
    data_atual = datetime.strptime(data_atual, "%Y-%m-%d")

    if data_validade < data_atual:
        logging.warning("Contrato vencido {}".format(token))
    else:    
        return empresas

if __name__ == '__main__':
    buscar_dados_id(token='ba8f9bc022e4cb0805b974d4f6cbd5a4')
