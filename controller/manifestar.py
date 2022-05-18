from pynfe.processamento.comunicacao import ComunicacaoSefaz
from pynfe.processamento.serializacao import SerializacaoXML
from pynfe.processamento.assinatura import AssinaturaA1
from pynfe.entidades.evento import EventoManifestacaoDest
from pynfe.entidades.fonte_dados import _fonte_dados
import datetime

certificado = r"C:\VersoesServico\CONCRESERRA\CONCRESERRA CONCRETO LTDA.pfx"
senha = '123456789AB'
uf = 'an'
homologacao = False

CNPJ = '10386032000120' 
CHAVE = ''
ultNSU = 0
maxNSU = 0
cStat = 0
NSU = 0


with open('./chaves.txt', 'r') as files:
        
    for linha in files:
        CHAVE = linha[0:44]
        manif_dest = EventoManifestacaoDest(
            cnpj=CNPJ,                                # cnpj do destinatário
            chave=CHAVE, # chave de acesso da nota
            data_emissao=datetime.datetime.now(),
            uf=uf,
            operacao=2                                       # - numero da operacao 
                                                                    # 1=Confirmação da Operação
                                                                    # 2=Ciência da Emissão
                                                                    # 3=Desconhecimento da Operação
                                                                    # 4=Operação não Realizada
            )

        # serialização
        serializador = SerializacaoXML(_fonte_dados, homologacao=homologacao)
        nfe_manif = serializador.serializar_evento(manif_dest)

        # assinatura
        a1 = AssinaturaA1(certificado, senha)
        xml = a1.assinar(nfe_manif)

        con = ComunicacaoSefaz(uf, certificado, senha, homologacao)
        envio = con.evento(modelo='nfe', evento=xml)               # modelo='nfce' ou 'nfe'
        print(envio.text)
