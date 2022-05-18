import logging
import os 
from controller.lerconfig import lerconfig

config = lerconfig()
BackupXML = config['BackupXML']

depurar = config['depuracao']
depurar = depurar[0]
filename = depurar['filename']
filemode = depurar['filemode']
level = depurar['level']
print(level)
encode = depurar['encoding']
format
if not os.path.isdir('logs'):
    os.mkdir('logs')
if not os.path.isdir('temp'):
    os.mkdir('temp')
if not os.path.isdir('xml'):
    os.mkdir('xml')
if not os.path.isdir(BackupXML):
    os.mkdir(BackupXML)

if level == 'DEBUG':
    log_format = '%(asctime)s:%(levelname)s:%(filename)s:%(message)s'# Configuracao de logs
    logging.basicConfig(filename=filename,
                        filemode=filemode,
                        level=logging.DEBUG,
                        format=log_format) 
else:
    log_format = '%(asctime)s:%(levelname)s:%(filename)s:%(message)s'# Configuracao de logs
    logging.basicConfig(filename=filename,
                        filemode=filemode,
                        level=logging.INFO,
                        format=log_format) 