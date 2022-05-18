from xml.dom import minidom
import os
from docs.config import *
import shutil


config = lerconfig()
DT_Monitor = config['Diretorios_Monitorados']


def renomear(caminho=None, arquivo=None):
    if not caminho:
        return

    caminho_temp = r'./temp'

    try:
        chave_acesso = ''
        transportada = ''
        with open(f'{caminho}', 'r', encoding='UTF-8') as f:
            xml = minidom.parse(f)
            chave_acesso = xml.getElementsByTagName("chNFe")
            transportada = xml.getElementsByTagName("transporta")

            if chave_acesso == [] or chave_acesso == '':
                print("Não é nota {}".format(caminho))
                f.close()
                if not os.path.isfile(caminho):

                    shutil.move(caminho, caminho_temp)
                else:
                    os.remove(caminho)
                    return

            chave_acesso = chave_acesso[0].firstChild.data

            if transportada:
                try:

                    if transportada[0].childNodes[1]:
                        transportada = transportada[0].childNodes[1]
                        cnpj_obtido = transportada.firstChild.data
                        logging.warning("Nota transportada {} pelo cnpj ou empresa {}".format(
                            chave_acesso, cnpj_obtido))
                        print("Nota transportada {} pelo cnpj ou empresa {}".format(
                            chave_acesso, cnpj_obtido))
                    else:
                        print("Não é transportada pelo mesma rastreada")
                except (Exception, IndexError) as erro_2:
                    logging.exception(erro_2)
                    pass
            else:
                logging.info("Nota destinada {}".format(chave_acesso))
                print("Nota destinada {}".format(chave_acesso))
        try:
            
            arquivo_xml = f'{chave_acesso}.xml'
            arquivo_xml_2 = f'{DT_Monitor}\\{arquivo_xml}'
            if not os.path.isfile(arquivo_xml_2):
                os.rename(caminho, arquivo_xml)
                if chave_acesso:
                    return chave_acesso
            
        except FileExistsError as erro:
            shutil.copy(caminho, caminho_temp)
            logging.exception(erro)
            if chave_acesso:
                return chave_acesso
            else: 
                False
        
    except shutil.Error as erro:
        logging.exception(erro)
        shutil.copy(caminho, caminho_temp)
        os.remove(caminho)
        if chave_acesso:
            return chave_acesso
        else: 
            False
    except Exception as erro:
        logging.exception(erro)
        if chave_acesso:
            return chave_acesso
        print(erro)
        return chave_acesso


if __name__ == "__main__":

    arquivos = os.listdir(DT_Monitor)
    for arquivo in arquivos:
        renomear(caminho=DT_Monitor + '//' + arquivo)
