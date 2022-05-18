from time import time
from unicodedata import numeric
from pynfe.utils.flags import NAMESPACE_NFE
from lxml import etree
from docs.config import *
import datetime
from controller.db_data import DataBase, DataBaseSQL
from controller.lerconfig import lerconfig
import shutil



def InserirNfe(resumo=False, caminho=None, arquivo_xml=None, cnpj_resumo=None):

    config = lerconfig()
    db = DataBase()
    
    caminho_backuxml = config['BackupXML']
    arq_back_xml = os.listdir(caminho_backuxml)

    arquivos = os.listdir(caminho)

    if resumo == False:

        arquivo_absoluto = caminho + '\\' + arquivo_xml
    
        if not '.xml' in arquivo_xml:
            return
        try:
            resposta = etree.parse(arquivo_absoluto)
        except etree.XMLSyntaxError as erro:
            logging.exception(erro)
            return

        ns = {'ns': NAMESPACE_NFE}
        try:
            chave_acesso = resposta.xpath('//ns:nfeProc/ns:protNFe/ns:infProt/ns:chNFe', namespaces=ns)[0].text
            xml = r'{}.xml'.format(chave_acesso)
            if not xml in arq_back_xml:
                
                shutil.copy(r"{}\\{}".format(caminho, xml), caminho_backuxml)

            notafiscal = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:ide/ns:nNF', namespaces=ns)[0].text
            cnpj_emitente = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:CNPJ', namespaces=ns)
            if cnpj_emitente == []:
                cnpj_emitente = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:CPF', namespaces=ns)[0].text
            else:
                cnpj_emitente = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:CNPJ', namespaces=ns)[0].text
            
            emitente = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:xNome', namespaces=ns)[0].text
            
            ieemitente = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:IE', namespaces=ns)
            if ieemitente != numeric:
                ieemitente = ''
            else:
                ieemitente = str(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:IE', namespaces=ns)[0].text)

            totalnota = float(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:total/ns:ICMSTot/ns:vNF', namespaces=ns)[0].text)
            
            infadcinfcpl = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:infAdic/ns:infCpl', namespaces=ns)
            if infadcinfcpl == []:
                infadcinfcpl = ''
            else:
                infadcinfcpl = str(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:infAdic/ns:infCpl', namespaces=ns)[0].text)
            iddest = int(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:ide/ns:idDest', namespaces=ns)[0].text)
            indfinal = int(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:ide/ns:indFinal', namespaces=ns)[0].text)
            indpres = int(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:ide/ns:indPres', namespaces=ns)[0].text)
            base_icms = float(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:total/ns:ICMSTot/ns:vBC', namespaces=ns)[0].text)
            valor_icms = float(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:total/ns:ICMSTot/ns:vICMS', namespaces=ns)[0].text)
            base_st = float(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:total/ns:ICMSTot/ns:vBCST', namespaces=ns)[0].text)
            tpnf = int(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:ide/ns:tpNF', namespaces=ns)[0].text)

            # verificando se existe cpf ou cnpj do destinatario
            cnpj_dest =  resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:dest/ns:CNPJ', namespaces=ns)
            if cnpj_dest:
                cnpj_dest =  resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:dest/ns:CNPJ', namespaces=ns)[0].text
            else:
                cnpj_dest =  resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:dest/ns:CPF', namespaces=ns)
                if cnpj_dest:
                  cnpj_dest =  resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:dest/ns:CPF', namespaces=ns)[0].text  
            
            dataemissao = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:ide/ns:dhEmi', namespaces=ns)[0].text
            dataemissao = dataemissao[0:10] + ' ' + dataemissao[11:19]
            data_format = "%Y-%m-%d %H:%M:%S"
            dataemissao = datetime.datetime.strptime(dataemissao, data_format)
            natop = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:ide/ns:natOp', namespaces=ns)[0].text

            modfrete = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:transp/ns:modFrete', namespaces=ns)[0].text

            codigo_dest = db.executa_DQL(f"""select codigo_empresa  from empresa e where cpfcnpj = '{cnpj_dest}' and token is not null;""")
            if codigo_dest == []:
                return
            codigo_dest = codigo_dest[0][0]
            url_arquivo = (r'\Integracao_Consumo_xml' +'\\'+ arquivo_xml)#pegar onde o xml esta
            #url_arquivo = str('')#pegar onde o xml esta
            icmserrado = float(0)# pode mandar null
            iest = str(0)# mandar vazio
            inclusao = datetime.datetime.now()

        except Exception as erro:
            logging.exception(erro)
            logging.info(chave_acesso)
            return


        
        try:
            verifica_empresa = db.executa_DQL(f"SELECT CODIGO_EMPRESA FROM EMPRESA WHERE CPFCNPJ = '{cnpj_emitente}'")
            if verifica_empresa == []:
                bairro = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:xBairro', namespaces=ns)
                if bairro!= []:
                    bairro = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:xBairro', namespaces=ns)[0].text
                
                cidade = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:cMun', namespaces=ns)
                if cidade != []:
                    cidade = int(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:cMun', namespaces=ns)[0].text)
                nome_cidade = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:xMun', namespaces=ns)
                if nome_cidade != []:
                    nome_cidade = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:xMun', namespaces=ns)[0].text
                estado = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:UF', namespaces=ns)
                if estado != []:
                    estado = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:UF', namespaces=ns)[0].text
                pais = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:cPais', namespaces=ns)
                if pais != []:
                    pais = int(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:cPais', namespaces=ns)[0].text)
                longradouro = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:xLgr', namespaces=ns)[0].text
                numero_residencia = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:nro', namespaces=ns)
                if numero_residencia != []:
                    numero_residencia = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:nro', namespaces=ns)[0].text
                else:
                    numero_residencia = ''
                numero_telefone = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:fone', namespaces=ns)
                if numero_telefone != []:
                    numero_telefone = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:fone', namespaces=ns)[0].text
                else:
                    numero_telefone = ''
                nome_emit = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:xNome', namespaces=ns)[0].text
                
                codigo_bairro = db.executa_DQL(f"SELECT codigo_bairro FROM BAIRRO WHERE BAIRRO = '{bairro}'")
                if codigo_bairro: 
                    codigo_bairro = codigo_bairro[0][0]
                else:
                    codigo_bairro = 0
                if not codigo_bairro or codigo_bairro == 0:
        
                    codigo_cidade = db.executa_DQL(f"SELECT CODIGO_CIDADE FROM CIDADE WHERE CODIGO_CIDADE = {cidade}")
                    if codigo_cidade == []:
                        db.executa_DML(f"INSERT INTO cidade (codigo_cidade, cidade, inclusao) VALUES({cidade}, '{nome_cidade}', '{inclusao}');")

                codigo_cidade = db.executa_DQL(f"SELECT CODIGO_CIDADE FROM CIDADE WHERE CODIGO_CIDADE = {cidade}")
                if codigo_cidade != []: 
                    codigo_cidade = codigo_cidade[0][0]
                else:
                    codigo_cidade = 0

                codigo_estado = db.executa_DQL(f"SELECT CODIGO_ESTADO FROM ESTADO WHERE UF = '{estado}'")
                if codigo_estado != []: 
                    codigo_estado = codigo_estado[0][0]
                else:
                    codigo_estado = 0

                codigo_pais = db.executa_DQL(f"SELECT CODIGO_PAIS FROM PAIS WHERE CODIGO_PAIS = {pais}")
                if codigo_pais != []: 
                    codigo_pais = codigo_pais[0][0]
                else:
                    codigo_pais = 0

                tipo = 'G'
                
                db.executa_DML(f"""INSERT INTO public.empresa
                                    (codigo_bairro, codigo_cidade, codigo_estado, codigo_pais, cpfcnpj, ftp_usuario, ftp_senha, tipo, certificado_nfe, smtp, smtp_porta, smtp_usuario, smtp_senha, smtp_ssl, email_contador, logradouro, numero, telefone, ie, emailempresa, smtp_tsl, url_logo, nome, papel, codigo_matriz, transportadora, emailredirecionasaida, emailredirecionaentrada, hostimap, imapport, tipotributacao, ordem, tempomedioentrega, url_certificado, tipocertificado, senha_certificado, "token", origemcad, inscrmunicipal, inclusao, data_consulta, ultimonsu)
                                    VALUES({codigo_bairro}, {codigo_cidade}, {codigo_estado}, {codigo_pais}, '{cnpj_emitente}', NULL, NULL, '{tipo}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{longradouro}', '{numero_residencia}', '{numero_telefone}', '{ieemitente}', '', NULL, NULL, '{nome_emit}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{inclusao}', NULL, NULL);"""
                                    )
                codigo_emit = db.executa_DQL(f"SELECT CODIGO_EMPRESA FROM EMPRESA WHERE CPFCNPJ = '{cnpj_emitente}'")
                codigo_emit = codigo_emit[0][0]                   
            else:
                codigo_emit = verifica_empresa[0][0]

            chave_verificada_nfedestinadas = db.executa_DQL(f"SELECT CHAVE_ACESSO FROM NFEDESTINADAS WHERE CHAVE_ACESSO = '{chave_acesso}'")

            downloads = db.executa_DQL(f"SELECT DOWNLOAD FROM NFEDESTINADAS WHERE CHAVE_ACESSO = '{chave_acesso}'")
            if downloads:
                downloads = downloads[0][0]
                if downloads == '0' or downloads == '':
                    db.executa_DML(f"DELETE FROM NFEDESTINADAS WHERE CHAVE_ACESSO = '{chave_acesso}'")
                elif downloads == None:
                    db.executa_DML(f"DELETE FROM NFEDESTINADAS WHERE CHAVE_ACESSO = '{chave_acesso}'")
                    import time
                    time.sleep(1)
                    # isolando a inclusão da destinadas em um try except
                    try:
                        db.executa_DML(
                            f"""INSERT INTO nfedestinadas
                                (notafiscal, chave_acesso, situacao, dataemissao, cnpjemitente, emitente, ieemitente, totalnota, codigo_dest, nsu, ambiente, situacaomanifesto, mensagem, corgao, atencao, ciencia, desconhecimento, opnaorealizada, oprealizada, url_arquivo, download, contabilizada, cce, ccensu, ccepossui, ccedata, natop, dataentrada, cienciaprotocolo, desconhecimentoprotocolo, opnaorealizadaprotocolo, oprealizadaprotocolo, envmail, impressa, cte, ctepossui, ctesituacao, ctechave_acesso, infadcinfcpl, infadcinfadfisco, erro_cadastro_empresa, infcomplementar, situacaoverificada, iddest, indfinal, indpres, cstat_ciencia, cstat_op_realizada, cstat_op_nao_realizada, cstat_op_desconhece, icmserrado, base_icms, valor_icms, base_st, valor_st, ipierrado, serieerrado, valorerrado, dataemiserrado, notafiscalerrado, cnpjerrado, vicmsdeson, vfcpufdest, vicmsufdest, vicmsufremet, vprod, vfrete, vseg, vdesc, vii, vipi, vpis, vcofins, voutro, tpnf, marcada, observacao, iest, inclusao)
                                VALUES
                                ({notafiscal}, '{chave_acesso}', 1, '{dataemissao}', '{cnpj_emitente}', '{emitente}', '{ieemitente}', {totalnota}, {codigo_dest}, NULL, 1, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{url_arquivo}', 1, NULL, NULL, NULL, NULL, NULL, '{natop}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{infadcinfcpl}', '', NULL, NULL, NULL, {iddest}, {indfinal}, {indpres}, NULL, NULL, NULL, NULL, NULL, {icmserrado}, {base_icms}, {valor_icms}, {base_st}, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, {tpnf}, NULL, NULL, '{iest}', '{inclusao}');"""
                                )
                        print('nfedestinadas inserida {}'.format(chave_acesso))
                        logging.info('nfedestinadas inserida {}'.format(chave_acesso))
                    except Exception as erro_nfedestinadas:
                        logging.exception(erro_nfedestinadas)
                        logging.warning(f"Chave de acesso erro {chave_acesso}")
                        return

            if chave_verificada_nfedestinadas:
                chave_verificada_nfedestinadas = chave_verificada_nfedestinadas[0][0]
                if chave_acesso != chave_verificada_nfedestinadas:
                    # isolando a inclusão da destinadas em um try except
                    try:
                        db.executa_DML(
                            f"""INSERT INTO nfedestinadas
                                (notafiscal, chave_acesso, situacao, dataemissao, cnpjemitente, emitente, ieemitente, totalnota, codigo_dest, nsu, ambiente, situacaomanifesto, mensagem, corgao, atencao, ciencia, desconhecimento, opnaorealizada, oprealizada, url_arquivo, download, contabilizada, cce, ccensu, ccepossui, ccedata, natop, dataentrada, cienciaprotocolo, desconhecimentoprotocolo, opnaorealizadaprotocolo, oprealizadaprotocolo, envmail, impressa, cte, ctepossui, ctesituacao, ctechave_acesso, infadcinfcpl, infadcinfadfisco, erro_cadastro_empresa, infcomplementar, situacaoverificada, iddest, indfinal, indpres, cstat_ciencia, cstat_op_realizada, cstat_op_nao_realizada, cstat_op_desconhece, icmserrado, base_icms, valor_icms, base_st, valor_st, ipierrado, serieerrado, valorerrado, dataemiserrado, notafiscalerrado, cnpjerrado, vicmsdeson, vfcpufdest, vicmsufdest, vicmsufremet, vprod, vfrete, vseg, vdesc, vii, vipi, vpis, vcofins, voutro, tpnf, marcada, observacao, iest, inclusao)
                                VALUES
                                ({notafiscal}, '{chave_acesso}', 1, '{dataemissao}', '{cnpj_emitente}', '{emitente}', '{ieemitente}', {totalnota}, {codigo_dest}, NULL, 1, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{url_arquivo}', 1, NULL, NULL, NULL, NULL, NULL, '{natop}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{infadcinfcpl}', '', NULL, NULL, NULL, {iddest}, {indfinal}, {indpres}, NULL, NULL, NULL, NULL, NULL, {icmserrado}, {base_icms}, {valor_icms}, {base_st}, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, {tpnf}, NULL, NULL, '{iest}', '{inclusao}');"""
                                )
                        print('nfedestinadas inserida {}'.format(chave_acesso))
                        logging.info('nfedestinadas inserida {}'.format(chave_acesso))
                    except Exception as erro_nfedestinadas:
                        logging.exception(erro_nfedestinadas)
                        logging.warning(f"Chave de acesso erro {chave_acesso}")
                        return
            uf = ''
            notatop = ''
            
            chave_verificada_nfe = db.executa_DQL(f"SELECT CHAVE_ACESSO FROM NFE WHERE CHAVE_ACESSO = '{chave_acesso}'")
            if not chave_verificada_nfe or  chave_verificada_nfe==[]:
                    try:
                        db.executa_DML(
                                f"""
                                INSERT INTO public.nfe
                                    (codigo_emit, empresa, codigo_dest, codigo_cliente_fd, notafiscal, url_arquivo, valor_total, origem, enviar_contador, chave_acesso, status, versao, staus_schema, data_emissao, data_ent_saida, base_icms, valor_icms, base_st, valor_st, valor_produtos, valor_frete, valor_seguro, valor_desconto, valor_ii, valor_ipi, valor_pis, valor_cofins, valor_outros, cfop, diretorio, lote, uf_emi_dest, enviado_contador, statusschema, validado, envemail, backup, natop, dataentrada, codigo_transportadora, sincronizar, impressa, situacao, situacaomanifesto, atencao, contabilizada, cte, ctepossui, ctechave_acesso, modfrete, infadcinfcpl, infadcinfadfisco, erro_cadastro_empresa, ctesituacao, infcomplementar, iddest, indfinal, indpres, icmserrado, ipierrado, vicmsdeson, vfcpufdest, vicmsufdest, vicmsufremet, vprod, vfrete, vseg, vdesc, vii, vipi, vpis, vcofins, voutro, tpnf, backup_interno_transp, marcada, observacao, iest, modelo, serieerrado, valorerrado, dataemiserrado, notafiscalerrado, cnpjerrado, arquivo, inclusao)
                                    VALUES
                                    ({codigo_emit}, {codigo_dest}, {codigo_dest}, NULL, '{notafiscal}', '{url_arquivo}', {totalnota}, 1, NULL, '{chave_acesso}', NULL, '4.00', NULL, '{dataemissao}', '1899-12-30 00:00:00.000', 0.0, 0.0, 0.0, 0.0, {totalnota}, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, NULL, '{caminho}', NULL, '{uf}', NULL, NULL, 1, NULL, 0, '{notatop}', NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, {modfrete}, '{infadcinfcpl}', '', NULL, NULL, NULL, {iddest}, {indfinal}, {indpres}, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, {tpnf}, NULL, NULL, NULL, '', '55', NULL, NULL, NULL, NULL, NULL, NULL, '{inclusao}');"""
                                    )
                        print('nfe inserida {}'.format(chave_acesso))
                        logging.info('nfe inserida {}'.format(chave_acesso))
                    except Exception as erro_nfedestinadas:
                        logging.exception(erro_nfedestinadas)
                        logging.warning(f"Chave de acesso erro {chave_acesso}")
                        return
    
        except Exception as erro:
            logging.exception(erro)

    else:
        ns = {'ns': NAMESPACE_NFE}
        resposta = etree.parse(caminho +'\\'+ arquivo_xml)
        chave_acesso_res = resposta.xpath('//ns:resNFe/ns:chNFe', namespaces=ns)[0].text
        verificar = db.executa_DQL(f"SELECT CHAVE_ACESSO FROM NFEDESTINADAS WHERE CHAVE_ACESSO = '{chave_acesso_res}'")
        if verificar:
            return
        else:
            logging.info(chave_acesso_res)
            logging.info("Cadastrando Resumo NF-e")
            cnpj_emit = resposta.xpath('//ns:resNFe/ns:CNPJ', namespaces=ns)[0].text
            xNome = resposta.xpath('//ns:resNFe/ns:xNome', namespaces=ns)[0].text
            ie = resposta.xpath('//ns:resNFe/ns:IE', namespaces=ns)[0].text
            dhEmi = resposta.xpath('//ns:resNFe/ns:dhEmi', namespaces=ns)[0].text
            tpNF = resposta.xpath('//ns:resNFe/ns:tpNF', namespaces=ns)[0].text
            vNF = resposta.xpath('//ns:resNFe/ns:vNF', namespaces=ns)[0].text
            dhRecbto = resposta.xpath('//ns:resNFe/ns:dhRecbto', namespaces=ns)[0].text
            nProt = resposta.xpath('//ns:resNFe/ns:nProt', namespaces=ns)[0].text 
            cSitNFe = resposta.xpath('//ns:resNFe/ns:cSitNFe', namespaces=ns)[0].text
            numNota = chave_acesso_res[25:34]
            codigo_dest = db.executa_DQL(f"""select codigo_empresa  from empresa e where cpfcnpj = '{cnpj_resumo}' and token is not null;""")
            if codigo_dest:
                codigo_dest = codigo_dest[0][0]
            # codigo_dest = 4
            # eu cetei 4 pra testar  esqueci de tirar
            db.executa_DML(f"""
                            INSERT INTO NFEDESTINADAS( NotaFiscal, Chave_Acesso, Situacao, DataEmissao, CNPJEmitente,Emitente, IEEmitente, Codigo_dest, Ambiente, NSU, tpnf, situacaomanifesto, TOTALNOTA)
                            VALUES({numNota},'{chave_acesso_res}',{cSitNFe},'{dhEmi}','{cnpj_emit}','{xNome}','{ie}', {codigo_dest},1, '', {tpNF}, 0, {vNF});""")

#  sql server 


def InserirNfe_sql_server(resumo=False, caminho=None, arquivo_xml=None, cnpj_resumo=None):

    config = lerconfig()
    db = DataBaseSQL()
    
    caminho_backuxml = config['BackupXML']
    arq_back_xml = os.listdir(caminho_backuxml)

    arquivos = os.listdir(caminho)

    if resumo == False:

        arquivo_absoluto = caminho + '\\' + arquivo_xml
    
        if not '.xml' in arquivo_xml:
            return
        try:
            resposta = etree.parse(arquivo_absoluto)
        except etree.XMLSyntaxError as erro:
            logging.exception(erro)
            return

        ns = {'ns': NAMESPACE_NFE}
        try:
            chave_acesso = resposta.xpath('//ns:nfeProc/ns:protNFe/ns:infProt/ns:chNFe', namespaces=ns)
            if chave_acesso != []:
                chave_acesso = resposta.xpath('//ns:nfeProc/ns:protNFe/ns:infProt/ns:chNFe', namespaces=ns)[0].text
            else:
                return
            xml = r'{}.xml'.format(chave_acesso)
            if not xml in arq_back_xml:
                
                shutil.copy(r"{}\\{}".format(caminho, xml), caminho_backuxml)

            notafiscal = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:ide/ns:nNF', namespaces=ns)[0].text
            cnpj_emitente = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:CNPJ', namespaces=ns)
            if cnpj_emitente == []:
                cnpj_emitente = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:CPF', namespaces=ns)[0].text
            else:
                cnpj_emitente = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:CNPJ', namespaces=ns)[0].text
            
            emitente = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:xNome', namespaces=ns)[0].text
            
            ieemitente = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:IE', namespaces=ns)
            if ieemitente != numeric:
                ieemitente = ''
            else:
                ieemitente = str(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:IE', namespaces=ns)[0].text)

            totalnota = float(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:total/ns:ICMSTot/ns:vNF', namespaces=ns)[0].text)
            
            infadcinfcpl = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:infAdic/ns:infCpl', namespaces=ns)
            if infadcinfcpl == []:
                infadcinfcpl = ''
            else:
                infadcinfcpl = str(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:infAdic/ns:infCpl', namespaces=ns)[0].text)
            iddest = int(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:ide/ns:idDest', namespaces=ns)[0].text)
            indfinal = int(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:ide/ns:indFinal', namespaces=ns)[0].text)
            indpres = int(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:ide/ns:indPres', namespaces=ns)[0].text)
            base_icms = float(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:total/ns:ICMSTot/ns:vBC', namespaces=ns)[0].text)
            valor_icms = float(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:total/ns:ICMSTot/ns:vICMS', namespaces=ns)[0].text)
            base_st = float(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:total/ns:ICMSTot/ns:vBCST', namespaces=ns)[0].text)
            tpnf = int(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:ide/ns:tpNF', namespaces=ns)[0].text)

            # verificando se existe cpf ou cnpj do destinatario
            cnpj_dest =  resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:dest/ns:CNPJ', namespaces=ns)
            if cnpj_dest:
                cnpj_dest =  resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:dest/ns:CNPJ', namespaces=ns)[0].text
            else:
                cnpj_dest =  resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:dest/ns:CPF', namespaces=ns)
                if cnpj_dest:
                  cnpj_dest =  resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:dest/ns:CPF', namespaces=ns)[0].text  
            dataemissao = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:ide/ns:dhEmi', namespaces=ns)[0].text
            dataemissao = dataemissao[0:10] + ' ' + dataemissao[11:19]
            data_format = "%Y-%m-%d %H:%M:%S"
            dataemissao = datetime.datetime.strptime(dataemissao, data_format)
            natop = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:ide/ns:natOp', namespaces=ns)[0].text

            modfrete = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:transp/ns:modFrete', namespaces=ns)[0].text

            codigo_dest = db.executa_DQL(f"""select codigo_empresa  from empresa e where cpfcnpj = '{cnpj_dest}' and token is not null;""")
            if codigo_dest == []:
                return
            codigo_dest = codigo_dest[0][0]
            url_arquivo = (r'\Integracao_Consumo_xml' +'\\'+ arquivo_xml)#pegar onde o xml esta
            #url_arquivo = str('')#pegar onde o xml esta
            icmserrado = float(0)# pode mandar null
            iest = str(0)# mandar vazio
            # inclusao = datetime.datetime.now()
            inclusao = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        except Exception as erro:
            logging.exception(erro)
            logging.info(chave_acesso)
            return


        
        try:
            verifica_empresa = db.executa_DQL(f"SELECT CODIGO_EMPRESA FROM EMPRESA WHERE CPFCNPJ = '{cnpj_emitente}'")
            if verifica_empresa == []:
                bairro = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:xBairro', namespaces=ns)
                if bairro!= []:
                    bairro = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:xBairro', namespaces=ns)[0].text
                
                cidade = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:cMun', namespaces=ns)
                if cidade != []:
                    cidade = int(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:cMun', namespaces=ns)[0].text)
                nome_cidade = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:xMun', namespaces=ns)
                if nome_cidade != []:
                    nome_cidade = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:xMun', namespaces=ns)[0].text
                estado = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:UF', namespaces=ns)
                if estado != []:
                    estado = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:UF', namespaces=ns)[0].text
                pais = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:cPais', namespaces=ns)
                if pais != []:
                    pais = int(resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:cPais', namespaces=ns)[0].text)
                longradouro = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:xLgr', namespaces=ns)[0].text
                numero_residencia = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:nro', namespaces=ns)
                if numero_residencia != []:
                    numero_residencia = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:nro', namespaces=ns)[0].text
                else:
                    numero_residencia = ''
                numero_telefone = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:fone', namespaces=ns)
                if numero_telefone != []:
                    numero_telefone = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:enderEmit/ns:fone', namespaces=ns)[0].text
                else:
                    numero_telefone = ''
                nome_emit = resposta.xpath('//ns:nfeProc/ns:NFe/ns:infNFe/ns:emit/ns:xNome', namespaces=ns)[0].text
                
                codigo_bairro = db.executa_DQL(f"SELECT codigo_bairro FROM BAIRRO WHERE BAIRRO = '{bairro}'")
                if codigo_bairro: 
                    codigo_bairro = codigo_bairro[0][0]
                else:
                    codigo_bairro = 0
                if not codigo_bairro or codigo_bairro == 0:
        
                    codigo_cidade = db.executa_DQL(f"SELECT CODIGO_CIDADE FROM CIDADE WHERE CODIGO_CIDADE = {cidade}")
                    if codigo_cidade == []:
                        db.executa_DML(f"INSERT INTO cidade (codigo_cidade, cidade, inclusao) VALUES({cidade}, '{nome_cidade}', '{inclusao}');")

                codigo_cidade = db.executa_DQL(f"SELECT CODIGO_CIDADE FROM CIDADE WHERE CODIGO_CIDADE = {cidade}")
                if codigo_cidade != []: 
                    codigo_cidade = codigo_cidade[0][0]
                else:
                    codigo_cidade = 0

                codigo_estado = db.executa_DQL(f"SELECT CODIGO_ESTADO FROM ESTADO WHERE UF = '{estado}'")
                if codigo_estado != []: 
                    codigo_estado = codigo_estado[0][0]
                else:
                    codigo_estado = 0

                codigo_pais = db.executa_DQL(f"SELECT CODIGO_PAIS FROM PAIS WHERE CODIGO_PAIS = {pais}")
                if codigo_pais != []: 
                    codigo_pais = codigo_pais[0][0]
                else:
                    codigo_pais = 0

                tipo = 'G'
                
                db.executa_DML(f"""INSERT INTO empresa
                                    (codigo_bairro, codigo_cidade, codigo_estado, codigo_pais, cpfcnpj, ftp_usuario, ftp_senha, tipo, certificado_nfe, smtp, smtp_porta, smtp_usuario, smtp_senha, smtp_ssl, email_contador, logradouro, numero, telefone, ie, emailempresa, smtp_tsl, url_logo, nome, papel, codigo_matriz, transportadora, emailredirecionasaida, emailredirecionaentrada, hostimap, imapport, tipotributacao, ordem, tempomedioentrega, url_certificado, tipocertificado, senha_certificado, "token", origemcad, inscrmunicipal, inclusao, data_consulta, ultimonsu)
                                    VALUES({codigo_bairro}, {codigo_cidade}, {codigo_estado}, {codigo_pais}, '{cnpj_emitente}', NULL, NULL, '{tipo}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{longradouro}', '{numero_residencia}', '{numero_telefone}', '{ieemitente}', '', NULL, NULL, '{nome_emit}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{inclusao}', NULL, NULL);"""
                                    )
                codigo_emit = db.executa_DQL(f"SELECT CODIGO_EMPRESA FROM EMPRESA WHERE CPFCNPJ = '{cnpj_emitente}'")
                codigo_emit = codigo_emit[0][0]                   
            else:
                codigo_emit = verifica_empresa[0][0]

            chave_verificada_nfedestinadas = db.executa_DQL(f"SELECT CHAVE_ACESSO FROM NFEDESTINADAS WHERE CHAVE_ACESSO = '{chave_acesso}'")

            downloads = db.executa_DQL(f"SELECT DOWNLOAD FROM NFEDESTINADAS WHERE CHAVE_ACESSO = '{chave_acesso}'")
            if downloads:
                downloads = downloads[0][0]
                if downloads == '0' or downloads == '':
                    db.executa_DML(f"DELETE FROM NFEDESTINADAS WHERE CHAVE_ACESSO = '{chave_acesso}'")
                elif downloads == None:
                    db.executa_DML(f"DELETE FROM NFEDESTINADAS WHERE CHAVE_ACESSO = '{chave_acesso}'")
                    import time
                    time.sleep(1)
                    try:
                        sql_destinadas_no_down = f"""INSERT INTO nfedestinadas
                                (notafiscal, chave_acesso, situacao, dataemissao, cnpjemitente, emitente, ieemitente, totalnota, codigo_dest, nsu, ambiente, situacaomanifesto, mensagem, corgao, atencao, ciencia, desconhecimento, opnaorealizada, oprealizada, url_arquivo, download, contabilizada, cce, ccensu, ccepossui, ccedata, natop, dataentrada, cienciaprotocolo, desconhecimentoprotocolo, opnaorealizadaprotocolo, oprealizadaprotocolo, envmail, impressa, cte, ctepossui, ctesituacao, ctechave_acesso, infadcinfcpl, infadcinfadfisco, erro_cadastro_empresa, infcomplementar, situacaoverificada, iddest, indfinal, indpres, cstat_ciencia, cstat_op_realizada, cstat_op_nao_realizada, cstat_op_desconhece, icmserrado, base_icms, valor_icms, base_st, valor_st, ipierrado, serieerrado, valorerrado, dataemiserrado, notafiscalerrado, cnpjerrado, vicmsdeson, vfcpufdest, vicmsufdest, vicmsufremet, vprod, vfrete, vseg, vdesc, vii, vipi, vpis, vcofins, voutro, tpnf, marcada, observacao, iest, inclusao)
                                VALUES
                                ({notafiscal}, '{chave_acesso}', 1, '{dataemissao}', '{cnpj_emitente}', '{emitente}', '{ieemitente}', {totalnota}, {codigo_dest}, NULL, 1, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{url_arquivo}', 1, NULL, NULL, NULL, NULL, NULL, '{natop}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{infadcinfcpl}', '', NULL, NULL, NULL, {iddest}, {indfinal}, {indpres}, NULL, NULL, NULL, NULL, NULL, {icmserrado}, {base_icms}, {valor_icms}, {base_st}, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, {tpnf}, NULL, NULL, '{iest}', '{inclusao}');"""
                                
                        db.executa_DML(sql_destinadas_no_down)
                        print('nfedestinadas inserida {}'.format(chave_acesso))
                        logging.info('nfedestinadas inserida {}'.format(chave_acesso))
                    except Exception as erro_nfedestinadas:
                        logging.exception(erro_nfedestinadas)
                        logging.warning(f"Chave de acesso erro {chave_acesso}")
                        logging.info("SQl Destinadas erro" + sql_destinadas_no_down)
                        return

            if not chave_verificada_nfedestinadas:
                # chave_verificada_nfedestinadas = chave_verificada_nfedestinadas[0][0]
                if chave_acesso != chave_verificada_nfedestinadas:
                    try:
                        sql_destinadas = f"""INSERT INTO nfedestinadas
                                (notafiscal, chave_acesso, situacao, dataemissao, cnpjemitente, emitente, ieemitente, totalnota, codigo_dest, nsu, ambiente, situacaomanifesto, mensagem, corgao, atencao, ciencia, desconhecimento, opnaorealizada, oprealizada, url_arquivo, download, contabilizada, cce, ccensu, ccepossui, ccedata, natop, dataentrada, cienciaprotocolo, desconhecimentoprotocolo, opnaorealizadaprotocolo, oprealizadaprotocolo, envmail, impressa, cte, ctepossui, ctesituacao, ctechave_acesso, infadcinfcpl, infadcinfadfisco, erro_cadastro_empresa, infcomplementar, situacaoverificada, iddest, indfinal, indpres, cstat_ciencia, cstat_op_realizada, cstat_op_nao_realizada, cstat_op_desconhece, icmserrado, base_icms, valor_icms, base_st, valor_st, ipierrado, serieerrado, valorerrado, dataemiserrado, notafiscalerrado, cnpjerrado, vicmsdeson, vfcpufdest, vicmsufdest, vicmsufremet, vprod, vfrete, vseg, vdesc, vii, vipi, vpis, vcofins, voutro, tpnf, marcada, observacao, iest, inclusao)
                                VALUES
                                ({notafiscal}, '{chave_acesso}', 1, '{dataemissao}', '{cnpj_emitente}', '{emitente}', '{ieemitente}', {totalnota}, {codigo_dest}, NULL, 1, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{url_arquivo}', 1, NULL, NULL, NULL, NULL, NULL, '{natop}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{infadcinfcpl}', '', NULL, NULL, NULL, {iddest}, {indfinal}, {indpres}, NULL, NULL, NULL, NULL, NULL, {icmserrado}, {base_icms}, {valor_icms}, {base_st}, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, {tpnf}, NULL, NULL, '{iest}', '{inclusao}');"""
                                
                        db.executa_DML(sql_destinadas)
                        print('nfedestinadas inserida {}'.format(chave_acesso))
                        logging.info('nfedestinadas inserida {}'.format(chave_acesso))
                    except Exception as erro_nfedestinadas:
                        logging.exception(erro_nfedestinadas)
                        logging.warning(f"Chave de acesso erro {chave_acesso}")
                        logging.info("SQL destinadas " + sql_destinadas)
                        return

            uf = ''
            notatop = ''
            
            chave_verificada_nfe = db.executa_DQL(f"SELECT CHAVE_ACESSO FROM NFE WHERE CHAVE_ACESSO = '{chave_acesso}'")
            if not chave_verificada_nfe or  chave_verificada_nfe==[]:
                    try:
                        #sqlnfe = f"""
                        #        INSERT INTO nfe
                        #            (codigo_emit, empresa, codigo_dest, codigo_cliente_fd, notafiscal, url_arquivo, valor_total, origem, enviar_contador, chave_acesso, status, versao, staus_schema, data_emissao, data_ent_saida, base_icms, valor_icms, base_st, valor_st, valor_produtos, valor_frete, valor_seguro, valor_desconto, valor_ii, valor_ipi, valor_pis, valor_cofins, valor_outros, cfop, diretorio, lote, uf_emi_dest, enviado_contador, statusschema, validado, envemail, backup, natop, dataentrada, codigo_transportadora, sincronizar, impressa, situacao, situacaomanifesto, atencao, contabilizada, cte, ctepossui, ctechave_acesso, modfrete, infadcinfcpl, infadcinfadfisco, erro_cadastro_empresa, ctesituacao, infcomplementar, iddest, indfinal, indpres, icmserrado, ipierrado, vicmsdeson, vfcpufdest, vicmsufdest, vicmsufremet, vprod, vfrete, vseg, vdesc, vii, vipi, vpis, vcofins, voutro, tpnf, backup_interno_transp, marcada, observacao, iest, modelo, serieerrado, valorerrado, dataemiserrado, notafiscalerrado, cnpjerrado, arquivo, inclusao)
                        #            VALUES
                        #            ({codigo_emit}, {codigo_dest}, {codigo_dest}, NULL, '{notafiscal}', '{url_arquivo}', {totalnota}, 1, NULL, '{chave_acesso}', NULL, '4.00', NULL, '{dataemissao}', '1899-12-30 00:00:00.000', 0.0, 0.0, 0.0, 0.0, {totalnota}, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, NULL, '{caminho}', NULL, '{uf}', NULL, NULL, 1, NULL, 0, '{notatop}', NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, {modfrete}, '{infadcinfcpl}', '', NULL, NULL, NULL, {iddest}, {indfinal}, {indpres}, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, {tpnf}, NULL, NULL, NULL, '', '55', NULL, NULL, NULL, NULL, NULL, NULL, '{inclusao}');"""
                            
                        sqlnfe = f"""
                                INSERT INTO nfe
                                    (codigo_emit, empresa, codigo_dest, notafiscal, url_arquivo, valor_total, origem, chave_acesso, [status], versao, data_emissao, data_ent_saida, base_icms, valor_icms, base_st, valor_st, valor_produtos, valor_frete, valor_seguro, valor_desconto, valor_ii, valor_ipi, valor_pis, valor_cofins, valor_outros, cfop, diretorio, lote, uf_emi_dest, enviado_contador, statusschema, validado, envemail, [backup], natop, dataentrada, codigo_transportadora, sincronizar, impressa, situacao, situacaomanifesto, atencao, contabilizada, cte, ctepossui, ctechave_acesso, modfrete, infadcinfcpl, infadcinfadfisco, erro_cadastro_empresa, ctesituacao, infcomplementar, iddest, indfinal, indpres, icmserrado, ipierrado, vicmsdeson, vfcpufdest, vicmsufdest, vicmsufremet, vprod, vfrete, vseg, vdesc, vii, vipi, vpis, vcofins, voutro, tpnf, backup_interno_transp, marcada, observacao, iest, modelo, serieerrado, valorerrado, dataemiserrado, notafiscalerrado, cnpjerrado, arquivo, inclusao)
                                    VALUES
                                    ({codigo_emit}, {codigo_dest}, {codigo_dest},'{notafiscal}', '{url_arquivo}', {totalnota}, 1, '{chave_acesso}', NULL, '4.00', '{dataemissao}', NULL, 0.0, 0.0, 0.0, 0.0, {totalnota}, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, NULL, '{caminho}', NULL, '{uf}', NULL, NULL, 1, NULL, 0, '{notatop}', NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, {modfrete}, '{infadcinfcpl}', '', NULL, NULL, NULL, {iddest}, {indfinal}, {indpres}, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, {tpnf}, NULL, NULL, NULL, '', '55', NULL, NULL, NULL, NULL, NULL, NULL, '{inclusao}');"""

                        db.executa_DML(sqlnfe)
                        print('nfe inserida {}'.format(chave_acesso))
                        logging.info('nfe {}'.format(chave_acesso))
                    except Exception as erro_nfe:
                        logging.exception(erro_nfe)
                        logging.warning(f"Chave de acesso erro {chave_acesso}")
                        logging.info("Script SQL " + sqlnfe)
                        return
    
        except Exception as erro:
            logging.exception(erro)

    else:
        logging.info("Cadastrando Resumo NF-e")
        ns = {'ns': NAMESPACE_NFE}
        resposta = etree.parse(caminho +'\\'+ arquivo_xml)
        chave_acesso_res = resposta.xpath('//ns:resNFe/ns:chNFe', namespaces=ns)[0].text
        
        verificar = db.executa_DQL(f"SELECT CHAVE_ACESSO FROM NFEDESTINADAS WHERE CHAVE_ACESSO = '{chave_acesso_res}'")
        if verificar:
            return
        else:
            logging.info(chave_acesso_res)
            try:
                cnpj_emit = resposta.xpath('//ns:resNFe/ns:CNPJ', namespaces=ns)[0].text
                xNome = resposta.xpath('//ns:resNFe/ns:xNome', namespaces=ns)[0].text
                ie = resposta.xpath('//ns:resNFe/ns:IE', namespaces=ns)[0].text
                dataemissao = resposta.xpath('//ns:resNFe/ns:dhEmi', namespaces=ns)[0].text
                dataemissao = dataemissao[0:10] + ' ' + dataemissao[11:19]
                data_format = "%Y-%m-%d %H:%M:%S"
                dhEmi = datetime.datetime.strptime(dataemissao, data_format)
                
                tpNF = resposta.xpath('//ns:resNFe/ns:tpNF', namespaces=ns)[0].text
                vNF = resposta.xpath('//ns:resNFe/ns:vNF', namespaces=ns)[0].text
                dhRecbto = resposta.xpath('//ns:resNFe/ns:dhRecbto', namespaces=ns)[0].text
                nProt = resposta.xpath('//ns:resNFe/ns:nProt', namespaces=ns)[0].text 
                cSitNFe = resposta.xpath('//ns:resNFe/ns:cSitNFe', namespaces=ns)[0].text
                numNota = chave_acesso_res[25:34]
                codigo_dest = db.executa_DQL(f"""select codigo_empresa  from empresa e where cpfcnpj = '{cnpj_resumo}' and token is not null;""")
                if codigo_dest:
                    codigo_dest = codigo_dest[0][0]
                # codigo_dest = 4
                # eu cetei 4 pra testar  esqueci de tirar
                db.executa_DML(f"""
                                INSERT INTO NFEDESTINADAS( NotaFiscal, Chave_Acesso, Situacao, DataEmissao, CNPJEmitente,Emitente, IEEmitente, Codigo_dest, Ambiente, NSU, tpnf, situacaomanifesto, totalnota)
                                        VALUES({numNota},'{chave_acesso_res}',{cSitNFe},'{dhEmi}','{cnpj_emit}','{xNome}','{ie}', {codigo_dest},1, '', {tpNF}, 0, {vNF});""")
            except Exception as erro:
                logging.exception(erro)
                logging.warning("Erro no cadastro do Resumo NF-e")
                return