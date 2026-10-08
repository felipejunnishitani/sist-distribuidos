# Atividade 5 - demonstracao
# Sobe os 10 nos num terminal so e mostra a saida de todos junta.
# -----------------------------------
# Felipe Jun Nishitani - 822353
# Gabriel Araujo Streicher - 822485
#
# USO:
#   py demo.py                  a e i iniciam uma eleicao no mesmo instante (t=1)
#   py demo.py a=1 i=1          o mesmo, escrito por extenso
#   py demo.py i=1 a=6          o a inicia depois, ja sabendo da eleicao do i
#   py demo.py --resumo         so as linhas importantes
#



import subprocess
import threading
import time
import sys
import os
import re

NOS = 'abcdefghij'
TEMPO_MAX = 40
IMPORTANTES = ('INICIO', 'abandono', 'ignorad', 'FIM', 'LIDER')

aqui = os.path.dirname(os.path.abspath(__file__))
resumo = '--resumo' in sys.argv

fontes = {}
for a in sys.argv[1:]:
    if '=' in a:
        no, t = a.split('=')
        fontes[no] = t
if not fontes:
    fontes = {'a': '1', 'i': '1'}

lideres = {}     # no -> lider que ele conhece
trava = threading.Lock()


def mostra(p):
    for linha in p.stdout:
        linha = linha.rstrip()
        if 'Enter = iniciar' in linha:
            continue

        m = re.match(r't=\s*[\d.]+ (\w) \| (.*)', linha)
        if m:
            no, texto = m.groups()
            l = re.search(r'LIDER da eleicao \S+: (\w)|o melhor no e \[(\w),', texto)
            if l:
                with trava:
                    lideres[no] = l.group(1) or l.group(2)
            if resumo and not any(k in texto for k in IMPORTANTES):
                continue

        with trava:
            print(linha, flush=True)


procs = []
for n in NOS:
    cmd = [sys.executable, '-u', os.path.join(aqui, 'eleicao_sem_fio.py'), n]
    if n in fontes:
        cmd += ['--iniciar', fontes[n]]
    p = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, text=True)
    procs.append(p)
    threading.Thread(target=mostra, args=(p,), daemon=True).start()

print('fontes: %s\n' % ', '.join('%s em t=%s' % (n, t) for n, t in sorted(fontes.items())),
      flush=True)

try:
    fim = time.time() + TEMPO_MAX
    while time.time() < fim and len(lideres) < len(NOS):
        time.sleep(0.2)
    time.sleep(0.5)
except KeyboardInterrupt:
    pass
finally:
    for p in procs:
        p.kill()   # sem isso os nos ficam rodando e segurando as portas

print('')
print('lider que cada no conhece: %s'
      % '  '.join('%s=%s' % (n, lideres.get(n, '?')) for n in NOS))
if len(lideres) == len(NOS) and len(set(lideres.values())) == 1:
    print('todos os nos concordam: o lider e %s' % lideres['a'])
else:
    print('!!! os nos NAO concordam (ou algum nao recebeu o anuncio)')
