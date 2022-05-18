import json
def lerconfig():
    try:    
        with open('./config.json') as f:
            config_banco = json.load(f)
            return config_banco
    except Exception as erros:
        print(erros)

