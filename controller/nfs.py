from pynfe.processamento.comunicacao import ComunicacaoNfse
from pynfe.processamento.serializacao import SerializacaoNfse
from pynfe.entidades.emitente import Emitente

# prestador
emitente = Emitente(
    cnpj='39299870000149',
    inscricao_municipal='1234'
    )

certificado = "E:\FiscalDefender_Mariano\APP\Certificado\MARIANO E THOMES COMERCIO DE ALIMENTOS LTDA(1).pfx"
senha = '14000333'
homologacao = True

serializador = SerializacaoNfse('ginfes')
xml = serializador.consultar_situacao_lote(emitente, '5')

con = ComunicacaoNfse(certificado, senha, 'ginfes', homologacao)
resposta = con.consultar_situacao_lote(xml)

print (resposta)