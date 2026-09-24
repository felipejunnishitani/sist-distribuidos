# Atividade 2 - Sistemas Distribuidos
# Exclusao mutua - Ricart & Agrawala
# -----------------------------------
# Felipe Jun Nishitani - 822353
# Gabriel Araujo Streicher - 822485
#
# USO:
#   py mutex.py <meu_id> [opcoes]
#
# OPCOES:
#   --pedir t1,t2,..  pede a SC nos instantes t1, t2.. (segundos depois de conectar)
#                     sem essa opcao o processo fica interativo: Enter = pedir a SC
#   --atraso seg      atraso de rede simulado em toda mensagem
#   --dura seg        encerra o processo depois de 'seg' segundos (sem ela: Ctrl+C)


import socket
import threading
import queue
import json
import time
import sys
import os

HOST = '127.0.0.1'
PORTA_BASE = 5000 # processo i escuta na porta PORTA_BASE + i
N = 3
ARQ_RECURSO = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'recurso.txt')


#####################
#### argumentos #####
#####################
def uso():
    print('uso: py mutex.py <meu_id 1..3> [--pedir t1,t2] [--atraso seg] [--dura seg]')
    sys.exit(1)


meu_id = 0
outros = []
PEDIDOS = [] # instantes em que vou pedir a SC
TEMPO_SC = 3.0 # tempo que cada um fica dentro da SC
ATRASO = 1.0 # atraso de rede, para dar tempo das mensagens se cruzarem
DURA = None


def le_args():
    global meu_id, outros, PEDIDOS, ATRASO, DURA
    if len(sys.argv) < 2:
        uso()
    meu_id = int(sys.argv[1])
    if not (1 <= meu_id <= N):
        print('meu_id precisa estar entre 1 e %d' % N)
        sys.exit(1)
    outros = [p for p in range(1, N + 1) if p != meu_id]

    i = 2
    while i < len(sys.argv):
        op = sys.argv[i]
        if i + 1 >= len(sys.argv):
            uso()
        val = sys.argv[i + 1]
        if op == '--pedir':
            PEDIDOS = [float(t) for t in val.split(',')]
        elif op == '--atraso':
            ATRASO = float(val)
        elif op == '--dura':
            DURA = float(val)
        else:
            print('opcao desconhecida: %s' % op)
            uso()
        i += 2


#####################
##### estado ########
#####################
relogio = 0
estado = 'FORA'
meu_pedido = None
oks = 0
fila = []
vezes = 0

trava = threading.Condition()
inicio = time.time()


def log(texto):
    print('t=%5.2f p%d [c=%2d] %-9s| %s'
          % (time.time() - inicio, meu_id, relogio, estado, texto), flush=True)


def nome(ts, de):
    return '(ts=%d,p%d)' % (ts, de)


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
        s.bind((HOST, PORTA_BASE + meu_id))
    except OSError:
        print('p%d: porta %d em uso (tem outro p%d rodando?)'
              % (meu_id, PORTA_BASE + meu_id, meu_id), flush=True)
        os._exit(1)
    s.listen(N)
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
    for p in outros:
        while True:
            try:
                s = socket.socket()
                s.connect((HOST, PORTA_BASE + p))
                conexoes[p] = s
                break
            except OSError:
                time.sleep(0.2)


def envia_loop(p):
    while True:
        quando, msg = saida[p].get()
        espera = quando - time.time()
        if espera > 0:
            time.sleep(espera)
        try:
            conexoes[p].sendall((json.dumps(msg) + '\n').encode())
        except OSError:
            return


def envia(p, tipo):
    saida[p].put((time.time() + ATRASO, {'tipo': tipo, 'ts': relogio, 'de': meu_id}))


#####################
##### algoritmo #####
#####################
def pedir_sc():
    global estado, relogio, meu_pedido, oks
    with trava:
        relogio += 1
        estado = 'ESPERANDO'
        meu_pedido = (relogio, meu_id)
        oks = 0
        log('quero a SC -> REQUEST%s para %s'
            % (nome(*meu_pedido), ['p%d' % p for p in outros]))
        for p in outros:
            envia(p, 'REQUEST')

        while oks < N - 1:
            trava.wait()

        estado = 'DENTRO'
        log('ENTREI NA SC (%d OKs)' % oks)

    usa_recurso()
    sair_sc()


def usa_recurso():
    with open(ARQ_RECURSO) as f:
        conteudo = f.read().strip()
    if conteudo != 'livre':
        log('!!! VIOLACAO: recurso estava "%s"' % conteudo)
    with open(ARQ_RECURSO, 'w') as f:
        f.write('ocupado por p%d' % meu_id)

    time.sleep(TEMPO_SC)

    with open(ARQ_RECURSO, 'w') as f:
        f.write('livre')


def sair_sc():
    global estado, relogio, fila, vezes
    with trava:
        estado = 'FORA'
        vezes += 1
        if fila:
            relogio += 1
            log('SAI DA SC -> mando OK para quem estava na fila %s'
                % ['p%d' % p for p in fila])
            for p in fila:
                envia(p, 'OK')
            fila = []
        else:
            log('SAI DA SC (fila vazia)')


def trata(msg):
    global relogio, oks
    with trava:
        relogio = max(relogio, msg['ts']) + 1
        de = msg['de']

        if msg['tipo'] == 'REQUEST':
            pedido = (msg['ts'], de)
            if estado == 'DENTRO':
                resp, motivo = 'NEGADO', 'estou na SC'
            elif estado == 'ESPERANDO' and meu_pedido < pedido:
                resp, motivo = 'NEGADO', 'meu pedido %s ganha' % nome(*meu_pedido)
            elif estado == 'ESPERANDO':
                resp, motivo = 'OK', 'o pedido dele ganha do meu %s' % nome(*meu_pedido)
            else:
                resp, motivo = 'OK', 'nao quero a SC'

            if resp == 'NEGADO':
                fila.append(de)
            relogio += 1
            log('recebi REQUEST%s de p%d -> %s (%s) | fila=%s'
                % (nome(*pedido), de, resp, motivo, ['p%d' % p for p in fila]))
            envia(de, resp)

        elif msg['tipo'] == 'OK':
            oks += 1
            log('recebi OK de p%d (%d/%d)' % (de, oks, N - 1))
            trava.notify_all()

        else:
            log('recebi NEGADO de p%d -> espero o OK dele depois' % de)


def trata_loop():
    while True:
        trata(recebidas.get())


def interativo():
    print('p%d: Enter = pedir a SC | q + Enter = sair' % meu_id, flush=True)
    while True:
        try:
            cmd = input().strip()
        except EOFError:
            return
        if cmd == 'q':
            return
        pedir_sc()


#####################
######## main #######
#####################
def main():
    global inicio
    le_args()

    if meu_id == 1 or not os.path.exists(ARQ_RECURSO):
        with open(ARQ_RECURSO, 'w') as f:
            f.write('livre')

    threading.Thread(target=escuta, args=(abre_porta(),), daemon=True).start()
    conecta()
    for p in outros:
        saida[p] = queue.Queue()
        threading.Thread(target=envia_loop, args=(p,), daemon=True).start()

    # espera todo mundo conectar em mim antes de comecar
    for _ in outros:
        entradas_prontas.acquire()
    threading.Thread(target=trata_loop, daemon=True).start()

    inicio = time.time()
    log('conectado com %s' % ['p%d' % p for p in outros])

    if PEDIDOS:
        for t in PEDIDOS:
            espera = inicio + t - time.time()
            if espera > 0:
                time.sleep(espera)
            pedir_sc()
    elif DURA is None:
        interativo()
        log('fim: entrei %d vez(es) na SC' % vezes)
        return

    # continua respondendo os outros ate acabar o tempo
    if DURA:
        resto = inicio + DURA - time.time()
        if resto > 0:
            time.sleep(resto)
    else:
        while True:
            time.sleep(1)



if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt: # Ctrl+C
        os._exit(0)
