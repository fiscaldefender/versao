from xml.dom import minidom
import os
from docs.config import *
import logging
import shutil

def renomear(caminho=None):
    if not caminho:
        return

    caminho_temp = './temp'

    try:    
        
        with open(f'{caminho}', 'r', encoding='UTF-8') as f:
            xml = minidom.parse(f)
            chave_acesso = xml.getElementsByTagName("chNFe")
            cnpj_emitente = xml.getElementsByTagName("CNPJ")
            nome_emitente = xml.getElementsByTagName("xNome")
            ie_emitente = xml.getElementsByTagName("IE")
            dt_emissao = xml.getElementsByTagName("dhEmi")
            tpNF = xml.getElementsByTagName("tpNF")
            valornfe = xml.getElementsByTagName("vNF")
            
            {numero_nota}, '{chave_acesso}', {situacao_inteiro}, '{data_emissao}', '{cnpj_emitente}', '{emitente}', 'ie_emitente', {total_nota}, {codigo_dest}, '{nsu}', {ambiente}, {situacao},

            if chave_acesso == []:
                print("Não é nota {}".format(caminho))
                f.close()
                shutil.move(caminho, caminho_temp)
                return
            
            chave_acesso = chave_acesso[0].firstChild.data

            if transportada:
                if transportada[0].childNodes[1]:
                    transportada = transportada[0].childNodes[1]
                    cnpj_obtido = transportada.firstChild.data
                else:
                    print("Não é transportada pelo mesma rastreada")
                    

                logging.warning("Nota transportada {} pelo cnpj ou empresa {}".format(chave_acesso, cnpj_obtido))
                print("Nota transportada {} pelo cnpj ou empresa {}".format(chave_acesso, cnpj_obtido))
            else:
                logging.info("Nota destinada {}".format(chave_acesso))
            print(chave_acesso)
            
        os.rename(caminho, f'./xml/{chave_acesso}.xml')
        if chave_acesso:
            return chave_acesso
    except shutil.Error as erro:
            logging.exception(erro)
            shutil.copy(caminho, caminho_temp)
            os.remove(caminho)
            return
    except Exception as erro:
        logging.exception(erro)
        if chave_acesso:
            return chave_acesso
        print(erro)
        return chave_acesso

if __name__ == "__main__":   
    caminhoold = r'E:\DEV_RES\Consumo_Indevido\xml'
    arquivos = os.listdir(caminhoold)
    for arquivo in arquivos:
        renomear(caminho=caminhoold + '//'+ arquivo)