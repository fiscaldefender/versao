import json
import configparser
config_po = r'C:\Backup ssd-hp\FD\FiscalDefender\config.ini'


config = configparser.ConfigParser(allow_no_value=True)
config.read(config_po)

_BANCO = config.get("CONFIG", "BANCO")
_USUARIO = config.get("CONFIG", "USUARIO")
_SENHA = config.get("CONFIG", "SENHA")
_DIRETORIOSINC = config.get("CONFIG", "DIRETORIOSINC")
_SERVIDOR = config.get("CONFIG", "SERVIDOR")
_PORTABANCO = config.get("CONFIG", "PORTABANCO")
_DIRMONITORADO1 = config.get("CONFIG", "DIRMONITORADO1")
_BASEDADOS = config.get("CONFIG", "BASEDADOS")

dict_ = {
    "host": _SERVIDOR,
    "user": _USUARIO,
    "password": "Weslei25*",
    "port": _PORTABANCO,
    "database": _BANCO,
    "ciencia": "",
    "tipo_database": _BASEDADOS,
    "filename": "Cataloga_FD.log",
    "filemode": "a",
    "level": "logging.DEBUG",
    "format": "log_format",
    "encoding": "UTF-8",
    "data_cataloga": "s",
    "intervalo_dia_cataloga_notas": "9",
    "tempo_exec_consulta": "1",
    "tempo_exec": "60",
    "INSTANCIA": "",
    "CERTIFICADO": "",
    "SENHA_CERTIFICADO": "",
    "CPFCNPJ": "",
    "TEMPO_EXECUSAO_HORAS_ROTINAS": "",
    "CAMINHO_LOGXML_FD": "",
    "Certificado": "",
    "BackupXML": _DIRETORIOSINC,
    "Diretorios_Monitorados": _DIRMONITORADO1
}
json_objct = json.dumps(dict_)
with open("config_teste.json", "w") as f:
    f.write(json_objct)
