import string
import random

chave = ''.join(random.choice(string.ascii_lowercase + string.ascii_uppercase + string.digits + '^!\$%&/()=?{[]}+~#-_.:,;<>|\\') for _ in range(0, 1024))

print(chave)

mensagem = 'python'

print("Msg: " + mensagem + '\n')

def str_xor(s1, s2):
  return "".join([chr(ord(c1) ^ ord(c2)) for (c1, c2) in zip(s1,s2)])

secreta = str_xor(mensagem, chave)

print("Mensagem secreta :"+secreta + "\n")

recuperando_mensagem_secreta = str_xor(secreta, chave)
print("Mensagem descriptografada " + "\n" + recuperando_mensagem_secreta + "\n")