from pynfe.processamento.comunicacao import ComunicacaoSefaz
from pynfe.utils.descompactar import DescompactaGzip
from pynfe.utils.flags import NAMESPACE_NFE
from lxml import etree
from controller.renomear import renomear
from docs.config import *
import datetime
from controller.db_data import DataBase, DataBaseSQL
from controller.lerconfig import lerconfig
from controller.inserir_nfe_resumo import InserirNfe , InserirNfe_sql_server


def consultar_nfe(certificado, senha, uf=None, homologacao=False, CPFCNPJ=None, NSU=0, CHAVE=''):

    config = lerconfig()
    caminho_xml = config['Diretorios_Monitorados']


    try:
        con = ComunicacaoSefaz(uf, certificado, senha, homologacao)
    except Exception as erro:
        logging.exception(erro)
        return
    ultNSU = 0
    maxNSU = 0
    cStat = 0
    data = datetime.datetime.now()
    db = DataBase()
    while True:
        xml = con.consulta_distribuicao(cnpj=CPFCNPJ, chave=CHAVE, nsu=NSU)
        NSU = str(NSU).zfill(15)
        print(f'Nova consulta a partir do NSU: {NSU}')
        logging.info(f'Nova consulta a partir do NSU: {NSU}')

        caminhogzip = f'{caminho_xml}/consulta_distrib_gzip-{NSU}.xml'
        with open(caminhogzip, 'w+') as f:
            f.write(xml.text)

        #resposta = etree.fromstring(xml.content)
        # print(resposta)
        resposta = etree.parse(
            f'{caminho_xml}/consulta_distrib_gzip-{NSU}.xml')
            
        ns = {'ns': NAMESPACE_NFE}

        contador_resposta = len(resposta.xpath(
            '//ns:retDistDFeInt/ns:loteDistDFeInt/ns:docZip', namespaces=ns))
        print(f'Quantidade de NSUs na consulta atual: {contador_resposta}')
        logging.info(
            f'Quantidade de NSUs na consulta atual: {contador_resposta}')

        cStat = resposta.xpath('//ns:retDistDFeInt/ns:cStat', namespaces=ns)
        if cStat != []:
            cStat[0].text
        else:
            logging.warning("cStat não encontrado, continuar da proxima consulta.")
            continue
        print(f'cStat: {cStat}')
        logging.info(f'cStat: {cStat}')

        xMotivo = resposta.xpath(
            '//ns:retDistDFeInt/ns:xMotivo', namespaces=ns)[0].text
        print(f'xMotivo: {xMotivo}')
        logging.info(f'xMotivo: {xMotivo}')

        maxNSU = resposta.xpath(
            '//ns:retDistDFeInt/ns:maxNSU', namespaces=ns)[0].text
        print(f'maxNSU: {maxNSU}')
        logging.info(f'maxNSU: {maxNSU}')

        # 137=nao tem mais arquivos e 138=existem mais arquivos para baixar
        if (cStat == '138'):
            for contador_xml in range(contador_resposta):
                tipo_schema = resposta.xpath(
                    '//ns:retDistDFeInt/ns:loteDistDFeInt/ns:docZip/@schema', namespaces=ns)[contador_xml]
                numero_nsu = resposta.xpath(
                    '//ns:retDistDFeInt/ns:loteDistDFeInt/ns:docZip/@NSU', namespaces=ns)[contador_xml]

                #nfe = 'procNFe_v4.00.xsd'
                #evento = 'procEventoNFe_v1.00.xsd'
                #resumo = 'resNFe_v1.01.xsd'
                if (tipo_schema == 'procNFe_v4.00.xsd'):
                    zip_resposta = resposta.xpath(
                        '//ns:retDistDFeInt/ns:loteDistDFeInt/ns:docZip', namespaces=ns)[contador_xml].text

                    descompactar_resposta = DescompactaGzip.descompacta(
                        zip_resposta)
                    texto_descompactado = etree.tostring(
                        descompactar_resposta).decode('utf-8')
                    caminho_arquivo = f'{caminho_xml}/consulta_distrib-nsu-{NSU}-contador-{contador_xml}.xml'
                    with open(caminho_arquivo, 'w+', encoding='UTF-8') as f:
                        f.write(texto_descompactado)
                    
                    # renomeando no modulo que rastreia
                    resposta_chave = etree.parse(caminho_arquivo)
                    chave_acesso = resposta_chave.xpath('//ns:nfeProc/ns:protNFe/ns:infProt/ns:chNFe', namespaces=ns)[0].text
                    arquivo_tratado = f'{caminho_xml}/{chave_acesso}.xml'
                    if os.path.isfile(arquivo_tratado):
                        os.remove(arquivo_tratado)
                    os.rename(caminho_arquivo, arquivo_tratado)
                    
                    
                    if len(chave_acesso) < 44:
                        logging.info("Chave de acesso invalida")
                        continue
                    else:
                        sql_nsu = f"""
                            INSERT INTO public.nsulista
                            (codigo_empresa, nsu, "data", validado, tpevento, codigo_rotina_lista, nfechaveacesso, erro_consulta, modelo)
                            VALUES((SELECT codigo_empresa from EMPRESA where CPFCNPJ ='{CPFCNPJ}'), '{numero_nsu}', '{data}', 0, 0, 0, '{chave_acesso}', 0, '55');
                            """
                        db.executa_DML(sql=sql_nsu)
                        arquivo_xml_func = chave_acesso + '.xml'

                        InserirNfe(caminho=caminho_xml, arquivo_xml=arquivo_xml_func)


                #resumo = 'resNFe_v1.01.xsd'           
                if (tipo_schema == 'resNFe_v1.01.xsd'):
                    zip_resposta = resposta.xpath(
                        '//ns:retDistDFeInt/ns:loteDistDFeInt/ns:docZip', namespaces=ns)[contador_xml].text

                    descompactar_resposta = DescompactaGzip.descompacta(
                        zip_resposta)
                    texto_descompactado = etree.tostring(
                        descompactar_resposta).decode('utf-8')
                    caminho_arquivo = f'{caminho_xml}/consulta_distrib-nsu-{NSU}-contador-{contador_xml}.xml'
                    with open(caminho_arquivo, 'w+', encoding='UTF-8') as f:
                        f.write(texto_descompactado)

                    # renomeando no modulo que rastreia
                    resposta_chave = etree.parse(caminho_arquivo)
                    chave_acesso = resposta_chave.xpath('//ns:resNFe/ns:chNFe', namespaces=ns)[0].text
                    arquivo_tratado = f'{caminho_xml}/{chave_acesso}.xml'
                    if os.path.isfile(arquivo_tratado):
                        os.remove(arquivo_tratado)
                    os.rename(caminho_arquivo, arquivo_tratado)

                    

                    if len(chave_acesso) < 44:
                        logging.info("Chave de acesso invalida")
                        continue
                    else:
                        nsu_verifica = f"select nsu  from nsulista where nsu = '{numero_nsu}' and codigo_empresa=(SELECT codigo_empresa from EMPRESA where CPFCNPJ ='{CPFCNPJ}');"
                        if nsu_verifica:
                            if nsu_verifica[0][0] == None or nsu_verifica[0][0] == '':
                                    
                                sql_nsu = f"""
                                    INSERT INTO public.nsulista
                                    (codigo_empresa, nsu, "data", validado, tpevento, codigo_rotina_lista, nfechaveacesso, erro_consulta, modelo)
                                    VALUES((SELECT codigo_empresa from EMPRESA where CPFCNPJ ='{CPFCNPJ}'), '{numero_nsu}', '{data}', 0, 0, 0, '{chave_acesso}', 0, '55');
                                    """
                                db.executa_DML(sql=sql_nsu)
                            local = config['Diretorios_Monitorados']

                        arquivo_xml_func = chave_acesso + '.xml'
                        InserirNfe(resumo=True,caminho=local, arquivo_xml=arquivo_xml_func, cnpj_resumo=CPFCNPJ)    

                else:
                    nsu_verifica = f"select nsu  from nsulista where nsu = '{numero_nsu}' and codigo_empresa=(SELECT codigo_empresa from EMPRESA where CPFCNPJ ='{CPFCNPJ}');"
                    if nsu_verifica:
                        if nsu_verifica[0][0] == None or nsu_verifica[0][0] == '':
                            sql_nsu = f"""
                                        INSERT INTO public.nsulista
                                        (codigo_empresa, nsu, "data", validado, tpevento, codigo_rotina_lista, nfechaveacesso, erro_consulta, modelo)
                                        VALUES((SELECT codigo_empresa from EMPRESA where CPFCNPJ ='{CPFCNPJ}'), '{numero_nsu}', '{data}', 0, 0, 0, NULL, 0, '55');
                                        """
                            db.executa_DML(sql=sql_nsu)

            NSU = resposta.xpath(
                '//ns:retDistDFeInt/ns:ultNSU', namespaces=ns)[0].text
            print(f'NSU: {NSU}')

            if len(NSU) == 15 and NSU != '000000000000000':    
                db.executa_DML("UPDATE empresa SET ultimonsu='{NSU}' WHERE cpfcnpj = '{CPFCNPJ}';")
                logging.info("Nsu atual na tabela empresa {}".format(NSU))
            logging.info(f'NSU: {NSU}')

        elif (cStat == '137'):
            print(f'Não há mais documentos a pesquisar')
            logging.warning(f'Não há mais documentos a pesquisar')
            break
        else:
            print(f'Falha')
            logging.error(f'Falha')
            renomear(caminho=caminhogzip)
            break
    return [maxNSU, NSU, cStat]

# sql server

def consultar_nfe_sql_server(certificado, senha, uf=None, homologacao=False, CPFCNPJ=None, NSU=0, CHAVE=''):

    config = lerconfig()
    caminho_xml = config['Diretorios_Monitorados']
    # data pra por no sqlserver
    data = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    """
    certificado = r"C:\VersoesServico\CONCRESERRA\CONCRESERRA CONCRETO LTDA.pfx"
    senha = '123456789AB'
    uf = 'es'
    homologacao = False

    CPFCNPJ = '10386032000120' 
    """
    try:
        con = ComunicacaoSefaz(uf, certificado, senha, homologacao)
    except Exception as erro:
        logging.exception(erro)
        return
    ultNSU = 0
    maxNSU = 0
    cStat = 0
    # data = datetime.datetime.now()
    db = DataBaseSQL()
    while True:
        xml = con.consulta_distribuicao(cnpj=CPFCNPJ, chave=CHAVE, nsu=NSU)
        NSU = str(NSU).zfill(15)
        print(f'Nova consulta a partir do NSU: {NSU}')
        logging.info(f'Nova consulta a partir do NSU: {NSU}')

        caminhogzip = f'{caminho_xml}/consulta_distrib_gzip-{NSU}.xml'
        with open(caminhogzip, 'w+') as f:
            f.write(xml.text)

        #resposta = etree.fromstring(xml.content)
        # print(resposta)
        resposta = etree.parse(
            f'{caminho_xml}/consulta_distrib_gzip-{NSU}.xml')
            
        ns = {'ns': NAMESPACE_NFE}

        contador_resposta = len(resposta.xpath(
            '//ns:retDistDFeInt/ns:loteDistDFeInt/ns:docZip', namespaces=ns))
        print(f'Quantidade de NSUs na consulta atual: {contador_resposta}')
        logging.info(
            f'Quantidade de NSUs na consulta atual: {contador_resposta}')

        cStat = resposta.xpath(
            '//ns:retDistDFeInt/ns:cStat', namespaces=ns)[0].text
        print(f'cStat: {cStat}')
        logging.info(f'cStat: {cStat}')

        xMotivo = resposta.xpath(
            '//ns:retDistDFeInt/ns:xMotivo', namespaces=ns)[0].text
        print(f'xMotivo: {xMotivo}')
        logging.info(f'xMotivo: {xMotivo}')

        maxNSU = resposta.xpath(
            '//ns:retDistDFeInt/ns:maxNSU', namespaces=ns)[0].text
        print(f'maxNSU: {maxNSU}')
        logging.info(f'maxNSU: {maxNSU}')

        # 137=nao tem mais arquivos e 138=existem mais arquivos para baixar
        if (cStat == '138'):
            for contador_xml in range(contador_resposta):
                tipo_schema = resposta.xpath(
                    '//ns:retDistDFeInt/ns:loteDistDFeInt/ns:docZip/@schema', namespaces=ns)[contador_xml]
                numero_nsu = resposta.xpath(
                    '//ns:retDistDFeInt/ns:loteDistDFeInt/ns:docZip/@NSU', namespaces=ns)[contador_xml]

                #nfe = 'procNFe_v4.00.xsd'
                #evento = 'procEventoNFe_v1.00.xsd'
                #resumo = 'resNFe_v1.01.xsd'
                if (tipo_schema == 'procNFe_v4.00.xsd'):
                    zip_resposta = resposta.xpath(
                        '//ns:retDistDFeInt/ns:loteDistDFeInt/ns:docZip', namespaces=ns)[contador_xml].text

                    descompactar_resposta = DescompactaGzip.descompacta(
                        zip_resposta)
                    texto_descompactado = etree.tostring(
                        descompactar_resposta).decode('utf-8')
                    caminho_arquivo = f'{caminho_xml}/consulta_distrib-nsu-{NSU}-contador-{contador_xml}.xml'
                    with open(caminho_arquivo, 'w+', encoding='UTF-8') as f:
                        f.write(texto_descompactado)
                    
                    # renomeando no modulo que rastreia
                    resposta_chave = etree.parse(caminho_arquivo)
                    chave_acesso = resposta_chave.xpath('//ns:nfeProc/ns:protNFe/ns:infProt/ns:chNFe', namespaces=ns)[0].text
                    arquivo_tratado = f'{caminho_xml}/{chave_acesso}.xml'
                    if os.path.isfile(arquivo_tratado):
                        os.remove(arquivo_tratado)
                    os.rename(caminho_arquivo, arquivo_tratado)
                    
                    
                    if len(chave_acesso) < 44:
                        logging.info("Chave de acesso invalida")
                        continue
                    else:
                        sql_nsu = f"""
                            INSERT INTO nsulista
                            (codigo_empresa, nsu, "data", validado, tpevento, codigo_rotina_lista, nfechaveacesso, erro_consulta, modelo)
                            VALUES((SELECT codigo_empresa from EMPRESA where CPFCNPJ ='{CPFCNPJ}'), '{numero_nsu}', '{data}', 0, 0, 0, '{chave_acesso}', 0, '55');
                            """
                        db.executa_DML(sql=sql_nsu)
                        arquivo_xml_func = chave_acesso + '.xml'

                        InserirNfe_sql_server(caminho=caminho_xml, arquivo_xml=arquivo_xml_func)


                #resumo = 'resNFe_v1.01.xsd'           
                if (tipo_schema == 'resNFe_v1.01.xsd'):
                    zip_resposta = resposta.xpath(
                        '//ns:retDistDFeInt/ns:loteDistDFeInt/ns:docZip', namespaces=ns)[contador_xml].text

                    descompactar_resposta = DescompactaGzip.descompacta(
                        zip_resposta)
                    texto_descompactado = etree.tostring(
                        descompactar_resposta).decode('utf-8')
                    caminho_arquivo = f'{caminho_xml}/consulta_distrib-nsu-{NSU}-contador-{contador_xml}.xml'
                    with open(caminho_arquivo, 'w+', encoding='UTF-8') as f:
                        f.write(texto_descompactado)

                    # renomeando no modulo que rastreia
                    resposta_chave = etree.parse(caminho_arquivo)
                    chave_acesso = resposta_chave.xpath('//ns:resNFe/ns:chNFe', namespaces=ns)[0].text
                    arquivo_tratado = f'{caminho_xml}/{chave_acesso}.xml'
                    if os.path.isfile(arquivo_tratado):
                        os.remove(arquivo_tratado)
                    os.rename(caminho_arquivo, arquivo_tratado)

                    

                    if len(chave_acesso) < 44:
                        logging.info("Chave de acesso invalida")
                        continue
                    else:
                        nsu_verifica = f"select nsu  from nsulista where nsu = '{numero_nsu}' and codigo_empresa=(SELECT codigo_empresa from EMPRESA where CPFCNPJ ='{CPFCNPJ}');"
                        if nsu_verifica:
                            if nsu_verifica[0][0] == None or nsu_verifica[0][0] == '':
                                    
                                sql_nsu = f"""
                                            INSERT INTO nsulista
                                            (codigo_empresa, nsu, "data", validado, tpevento, codigo_rotina_lista, nfechaveacesso, erro_consulta, modelo)
                                            VALUES((SELECT codigo_empresa from EMPRESA where CPFCNPJ ='{CPFCNPJ}'), '{numero_nsu}', '{data}', 0, 0, 0, '{chave_acesso}', 0, '55');
                                            """
                                db.executa_DML(sql=sql_nsu)
                            local = config['Diretorios_Monitorados']

                        arquivo_xml_func = chave_acesso + '.xml'
                        InserirNfe_sql_server(resumo=True,caminho=local, arquivo_xml=arquivo_xml_func, cnpj_resumo=CPFCNPJ)    

                else:
                    nsu_verifica = f"select nsu  from nsulista where nsu = '{numero_nsu}' and codigo_empresa=(SELECT codigo_empresa from EMPRESA where CPFCNPJ ='{CPFCNPJ}');"
                    if nsu_verifica:
                        if nsu_verifica[0][0] == None or nsu_verifica[0][0] == '':
                            sql_nsu = f"""
                                        INSERT INTO nsulista
                                        (codigo_empresa, nsu, "data", validado, tpevento, codigo_rotina_lista, nfechaveacesso, erro_consulta, modelo)
                                        VALUES((SELECT codigo_empresa from EMPRESA where CPFCNPJ ='{CPFCNPJ}'), '{numero_nsu}', '{data}', 0, 0, 0, NULL, 0, '55');
                                        """
                            db.executa_DML(sql=sql_nsu)

            NSU = resposta.xpath(
                '//ns:retDistDFeInt/ns:ultNSU', namespaces=ns)[0].text
            print(f'NSU: {NSU}')
            logging.info(f'NSU: {NSU}')

            if len(NSU) == 15 and NSU != '000000000000000':    
                db.executa_DML("UPDATE empresa SET ultimonsu='{NSU}' WHERE cpfcnpj = '{CPFCNPJ}';")
                logging.info("Nsu atual na tabela empresa {}".format(NSU))
            logging.info(f'NSU: {NSU}')
            
        elif (cStat == '137'):
            print(f'Não há mais documentos a pesquisar')
            logging.warning(f'Não há mais documentos a pesquisar')
            break
        else:
            print(f'Falha')
            logging.error(f'Falha')
            renomear(caminho=caminhogzip)
            break
    return [maxNSU, NSU, cStat]
