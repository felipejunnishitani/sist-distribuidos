# Atividade 5 - Sistemas Distribuidos
# Eleicao de lider em ambiente sem fio (Tanenbaum, secao 5.4.6)
# -----------------------------------
# Felipe Jun Nishitani - 822353
# Gabriel Araujo Streicher - 822485
#
# USO:
#   py eleicao_sem_fio.py <no a..j> [opcoes]   (um terminal para cada no)
#
# OPCOES:
#   --iniciar t1,t2,..  inicia uma eleicao nos instantes t1, t2.. (segundos depois de conectar)
#                       sem essa opcao o no fica interativo: Enter = iniciar eleicao
#   --atraso seg        atraso de rede simulado em toda mensagem (padrao 1)
#
# A topologia e a capacidade de cada no sao as dos slides (figura 5.24 do livro).
# Cada no so conversa com os seus vizinhos (os nos no seu alcance).
# Sem falhas de processo nem de canal.


import socket
import threading
import queue
import json
import time
import sys

HOST = '127.0.0.1'
PORTA_BASE = 5000 # no a escuta na porta PORTA_BASE + 1, b na + 2, ...

# topologia dos slides
CAPACIDADE = {'a': 4, 'b': 6, 'c': 3, 'd': 2, 'e': 1,
              'f': 4, 'g': 2, 'h': 8, 'i': 5, 'j': 4}
ARESTAS = ['ab', 'aj', 'bc', 'bg', 'cd', 'ce', 'de',
           'df', 'ef', 'eg', 'gj', 'gh', 'hi', 'fi']

NOS = sorted(CAPACIDADE)
VIZINHOS = {n: [] for n in NOS}
for x, y in ARESTAS:
    VIZINHOS[x].append(y)
    VIZINHOS[y].append(x)


def porta(n):
    return PORTA_BASE + NOS.index(n) + 1


#####################
#### argumentos #####
#####################
def uso():
    print('uso: py eleicao_sem_fio.py <no a..j> [--iniciar t1,t2] [--atraso seg]')
    sys.exit(1)


eu = ''
vizinhos = []
INICIOS = []   # instantes em que vou iniciar uma eleicao
ATRASO = 1.0   # atraso de rede, para dar tempo das eleicoes se cruzarem


def le_args():
    global eu, vizinhos, INICIOS, ATRASO
    if len(sys.argv) < 2 or sys.argv[1] not in NOS:
        uso()
    eu = sys.argv[1]
    vizinhos = sorted(VIZINHOS[eu])

    i = 2
    while i < len(sys.argv):
        op = sys.argv[i]
        if i + 1 >= len(sys.argv):
            uso()
        val = sys.argv[i + 1]
        if op == '--iniciar':
            INICIOS = [float(t) for t in val.split(',')]
        elif op == '--atraso':
            ATRASO = float(val)
        else:
            print('opcao desconhecida: %s' % op)
            uso()
        i += 2


#####################
##### estado ########
#####################
maior_seq = 0      # maior numero de eleicao que ja vi
eleicao = None     # (seq, no que iniciou) da eleicao de que participo
pai = None         # de quem recebi essa ELEICAO primeiro (None = eu iniciei)
pendentes = set()  # vizinhos de quem ainda espero ACK
melhor = None      # (capacidade, no) melhor candidato da minha subarvore
lider = None       # (capacidade, no) lider eleito
eleicao_lider = None  # eleicao que definiu o lider (para repassar o anuncio uma vez so)

inicio = time.monotonic()


def log(texto):
    print('t=%5.2f %s | %s' % (time.monotonic() - inicio, eu, texto), flush=True)


def nome_e(e):
    return '(%d,%s)' % e


def nome_c(c):
    return '[%s,%d]' % (c[1], c[0]) # mesma notacao dos slides: [no,capacidade]


#####################
######## rede #######
#####################
recebidas = queue.Queue()
conexoes = {}
saida = {}
entradas_prontas = threading.Semaphore(0)


def abre_porta():
    s = socket.socket()
    try:
        s.bind((HOST, porta(eu)))
    except OSError:
        print('%s: porta %d em uso (tem outro %s rodando?)' % (eu, porta(eu), eu), flush=True)
        sys.exit(1)
    s.listen(len(NOS))
    return s


def escuta(s):
    while True:
        conn, _ = s.accept()
        entradas_prontas.release()
        threading.Thread(target=le, args=(conn,), daemon=True).start()


def le(conn):
    try:
        arq = conn.makefile('r')
        for linha in arq:
            recebidas.put(json.loads(linha))
    except OSError:
        pass


def conecta():
    for p in vizinhos:
        while True:
            try:
                s = socket.socket()
                s.connect((HOST, porta(p)))
                conexoes[p] = s
                break
            except OSError:
                time.sleep(0.2) # o vizinho ainda nao subiu, tenta de novo


def envia_loop(p):
    while True:
        quando, msg = saida[p].get()
        espera = quando - time.monotonic()
        if espera > 0:
            time.sleep(espera) # simula a demora da rede
        try:
            conexoes[p].sendall((json.dumps(msg) + '\n').encode())
        except OSError:
            return # o vizinho ja foi encerrado


def envia(p, tipo, **dados):
    msg = {'tipo': tipo, 'de': eu}
    msg.update(dados)
    saida[p].put((time.monotonic() + ATRASO, msg))


#####################
##### algoritmo #####
#####################
def inicia_eleicao():
    global maior_seq
    maior_seq += 1
    participa((maior_seq, eu), None)


def participa(e, de):
    # entra na eleicao e tendo 'de' como pai (de = None: eu sou a fonte)
    global eleicao, pai, pendentes, melhor
    if eleicao is not None and eleicao_lider != eleicao:
        log('abandono a eleicao %s, a %s e maior' % (nome_e(eleicao), nome_e(e)))
    eleicao, pai = e, de
    melhor = (CAPACIDADE[eu], eu)
    pendentes = set(vizinhos) - {de}

    if de is None:
        log('>>> INICIO a eleicao %s -> ELEICAO para %s' % (nome_e(e), sorted(pendentes)))
    else:
        log('ELEICAO %s de %s pela 1a vez: %s e meu pai -> repasso para %s'
            % (nome_e(e), de, de, sorted(pendentes)))
    for p in pendentes:
        envia(p, 'ELEICAO', eleicao=e)

    if not pendentes:
        responde_pai() # todos os vizinhos sao o pai: sou folha


def responde_pai():
    log('ACK para o pai %s com o melhor da minha subarvore %s' % (pai, nome_c(melhor)))
    envia(pai, 'ACK', eleicao=eleicao, melhor=melhor)


def anuncia(l, de):
    global lider, eleicao_lider
    lider, eleicao_lider = l, eleicao
    for p in vizinhos:
        if p != de:
            envia(p, 'LIDER', eleicao=eleicao, lider=l)


def trata(msg):
    global maior_seq, melhor
    tipo = msg['tipo']
    if tipo == 'INICIAR':
        inicia_eleicao()
        return

    de = msg['de']
    e = tuple(msg['eleicao'])
    maior_seq = max(maior_seq, e[0])

    if tipo == 'ELEICAO':
        if eleicao is None or e > eleicao:
            participa(e, de)
        elif e == eleicao:
            log('ELEICAO %s de %s: ja participo dela -> ACK sem candidato' % (nome_e(e), de))
            envia(de, 'ACK', eleicao=e, melhor=None)
        else:
            log('ELEICAO %s de %s ignorada: estou na %s, que e maior'
                % (nome_e(e), de, nome_e(eleicao)))

    elif tipo == 'ACK':
        if e != eleicao or de not in pendentes:
            log('ACK de %s da eleicao %s ignorado (nao e a minha atual)' % (de, nome_e(e)))
            return
        pendentes.discard(de)
        cand = tuple(msg['melhor']) if msg['melhor'] else None
        if cand and cand > melhor:
            melhor = cand
        log('ACK de %s com %s | melhor ate agora %s | faltam %s'
            % (de, nome_c(cand) if cand else '-', nome_c(melhor), sorted(pendentes)))
        if not pendentes:
            if pai is None:
                log('>>> FIM da eleicao %s: o melhor no e %s -> anuncio LIDER'
                    % (nome_e(eleicao), nome_c(melhor)))
                anuncia(melhor, None)
            else:
                responde_pai()

    elif tipo == 'LIDER':
        if e != eleicao or eleicao_lider == e:
            return # anuncio de outra eleicao ou repetido
        l = tuple(msg['lider'])
        log('>>> LIDER da eleicao %s: %s (capacidade %d)' % (nome_e(e), l[1], l[0]))
        anuncia(l, de)


def agenda():
    for t in INICIOS:
        espera = inicio + t - time.monotonic()
        if espera > 0:
            time.sleep(espera)
        recebidas.put({'tipo': 'INICIAR'})


def teclado():
    print('%s: Enter = iniciar eleicao | q + Enter = sair' % eu, flush=True)
    while True:
        try:
            cmd = input().strip()
        except EOFError:
            return
        if cmd == 'q':
            recebidas.put({'tipo': 'SAIR'})
            return
        recebidas.put({'tipo': 'INICIAR'})


#####################
######## main #######
#####################
def main():
    global inicio
    le_args()

    threading.Thread(target=escuta, args=(abre_porta(),), daemon=True).start()
    conecta()
    for p in vizinhos:
        saida[p] = queue.Queue()
        threading.Thread(target=envia_loop, args=(p,), daemon=True).start()

    # espera todos os vizinhos conectarem em mim antes de comecar
    for _ in vizinhos:
        entradas_prontas.acquire()

    inicio = time.monotonic()
    log('conectado | capacidade %d | vizinhos %s' % (CAPACIDADE[eu], vizinhos))

    if INICIOS:
        threading.Thread(target=agenda, daemon=True).start()
    else:
        threading.Thread(target=teclado, daemon=True).start()

    # so esta thread mexe no estado do algoritmo
    try:
        while True:
            try:
                msg = recebidas.get(timeout=0.5)
            except queue.Empty:
                continue
            if msg['tipo'] == 'SAIR':
                break
            trata(msg)
    except KeyboardInterrupt:
        pass

    if lider:
        log('fim | lider: %s (capacidade %d)' % (lider[1], lider[0]))
    else:
        log('fim | sem lider eleito')


if __name__ == '__main__':
    main()
