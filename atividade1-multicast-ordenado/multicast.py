# Atividade 1 - Sistemas Distribuidos
# Multicast totalmente ordenado
# -----------------------------------
# Felipe Jun Nishitani - 822353
# Gabriel Araujo Streicher - 822485
#
# USO:
#   py multicast.py <meu_id> <n_processos> [opcoes]
#
# OPCOES:
#   --atraso p:seg   segura por 'seg' segundos a PRIMEIRA mensagem enviada
#                    para o processo p (simula um enlace lento)
#   --espera seg     espera 'seg' segundos antes de enviar a propria mensagem
#                    (faz o relogio logico avancar antes do envio)
#   --total k        numero total de mensagens no sistema (padrao: n_processos)


import socket
import threading
import queue
import json
import time
import sys

HOST = '127.0.0.1'
PORTA_BASE = 5000 # processo i escuta na porta PORTA_BASE + i


#####################
#### argumentos #####
#####################
def uso():
    print('uso: python3 multicast.py <meu_id> <n_processos> '
          '[--atraso p:seg] [--espera seg] [--total k]')
    sys.exit(1)


if len(sys.argv) < 3:
    uso()

meu_id = int(sys.argv[1])
N = int(sys.argv[2])

ATRASOS = {} # segundos de atraso na primeira mensagem
ESPERA = 0.0 # atraso antes de enviar a propria mensagem
TOTAL = None # quantas mensagens serao entregues no total

i = 3
while i < len(sys.argv):
    op = sys.argv[i]
    if op == '--atraso':
        destino, seg = sys.argv[i + 1].split(':')
        ATRASOS[int(destino)] = float(seg)
        i += 2
    elif op == '--espera':
        ESPERA = float(sys.argv[i + 1])
        i += 2
    elif op == '--total':
        TOTAL = int(sys.argv[i + 1])
        i += 2
    else:
        print('opcao desconhecida: %s' % op)
        uso()

if not (1 <= meu_id <= N):
    print('meu_id precisa estar entre 1 e %d' % N)
    sys.exit(1)

if TOTAL is None:
    TOTAL = N # por padrao cada processo envia exatamente 1 mensagem

PORTAS = {p: PORTA_BASE + p for p in range(1, N + 1)}
outros = [p for p in PORTAS if p != meu_id]

MINHA_MENSAGEM = 'm%d' % meu_id


#####################
##### estado ########
#####################
relogio = 0
fila = []
acks = {}
entregues = 0

ordem_recepcao = []
ordem_entrega = []

recebidas = queue.Queue()
conexoes = {}
saida = {p: queue.Queue() for p in outros}
entradas_prontas = threading.Semaphore(0)


def log(texto):
    print(texto, flush=True)


def nome(ts, de):
    return '(ts=%d,p%d)' % (ts, de)


#####################
######## rede #######
#####################
def escuta():
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORTAS[meu_id]))
    s.listen(N)
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
                s.connect((HOST, PORTAS[p]))
                conexoes[p] = s
                break
            except OSError:
                time.sleep(0.2) # o outro ainda nao subiu, tenta de novo


def envia_loop(p):
    atraso = ATRASOS.get(p, 0)
    while True:
        msg = saida[p].get()
        if atraso:
            time.sleep(atraso) # so segura a primeira mensagem
            atraso = 0
        conexoes[p].sendall((json.dumps(msg) + '\n').encode())
        saida[p].task_done()


def multicast(msg):
    for p in outros:
        saida[p].put(msg)


#####################
##### algoritmo #####
#####################
def manda_msg(texto):
    global relogio
    relogio += 1

    # a mensagem tambem e entregue ao proprio remetente
    fila.append([relogio, meu_id, texto])
    fila.sort(key=lambda m: (m[0], m[1]))
    acks.setdefault((relogio, meu_id), [])

    ordem_recepcao.append(texto)
    multicast({'tipo': 'MSG', 'ts': relogio, 'de': meu_id, 'texto': texto})
    log('p%d: enviei "%s" com ts=%d | fila=%s'
        % (meu_id, texto, relogio, mostra_fila()))


def manda_ack(ts, de):
    global relogio
    relogio += 1
    acks.setdefault((ts, de), []).append(meu_id)
    multicast({'tipo': 'ACK', 'ts': relogio, 'de': meu_id,
               'ref_ts': ts, 'ref_de': de})


def trata(msg):
    global relogio
    relogio = max(relogio, msg['ts']) + 1

    if msg['tipo'] == 'MSG':
        fila.append([msg['ts'], msg['de'], msg['texto']])
        fila.sort(key=lambda m: (m[0], m[1]))
        acks.setdefault((msg['ts'], msg['de']), [])
        ordem_recepcao.append(msg['texto'])
        log('p%d: RECEBI msg %s de p%d %s | relogio=%d | fila=%s'
            % (meu_id, msg['texto'], msg['de'],
               nome(msg['ts'], msg['de']), relogio, mostra_fila()))
        manda_ack(msg['ts'], msg['de'])

    else:
        k = (msg['ref_ts'], msg['ref_de'])
        if k not in acks:
            log('p%d: !!! ack de p%d chegou ANTES da mensagem %s '
                '-- guardando ack'
                % (meu_id, msg['de'], nome(k[0], k[1])))
        # setdefault (e nao acks[k] = []) preserva acks orfaos ja guardados
        acks.setdefault(k, []).append(msg['de'])
        log('p%d: recebi ack de p%d sobre %s | relogio=%d | acks=%s'
            % (meu_id, msg['de'], nome(k[0], k[1]), relogio, acks[k]))

    entrega()


def entrega():
    global entregues
    while fila:
        ts, de, texto = fila[0]
        confirmaram = acks.get((ts, de), [])
        precisa = [p for p in PORTAS if p != de]

        if all(p in confirmaram for p in precisa):
            fila.pop(0)
            acks.pop((ts, de), None)
            entregues += 1
            ordem_entrega.append(texto)
            log('p%d: >>> ENTREGA %d: "%s" %s'
                % (meu_id, entregues, texto, nome(ts, de)))
        else:
            faltam = [p for p in precisa if p not in confirmaram]
            log('p%d: topo "%s" %s ainda aguarda ack de %s'
                % (meu_id, texto, nome(ts, de), faltam))
            break


def mostra_fila():
    return [('%s%s' % (m[2], nome(m[0], m[1]))) for m in fila]


#####################
######## main #######
#####################
def main():
    threading.Thread(target=escuta, daemon=True).start()
    conecta()
    for p in outros:
        threading.Thread(target=envia_loop, args=(p,), daemon=True).start()

    # espera receber conexao de todos os outros antes de comecar
    for _ in outros:
        entradas_prontas.acquire()
    time.sleep(0.3)

    log('p%d: conectado com %s (%d processos, %d mensagens esperadas)'
        % (meu_id, outros, N, TOTAL))

    if ESPERA:
        # continua processando o que chega, para o relogio logico avancar
        log('p%d: esperando %.1fs antes de enviar' % (meu_id, ESPERA))
        fim = time.time() + ESPERA
        while time.time() < fim:
            try:
                trata(recebidas.get(timeout=fim - time.time()))
            except (queue.Empty, ValueError):
                break

    manda_msg(MINHA_MENSAGEM)

    while entregues < TOTAL:
        trata(recebidas.get())

    log('')
    log('p%d: ORDEM DE RECEPCAO  = %s' % (meu_id, ordem_recepcao))
    log('p%d: ORDEM DE ENTREGA   = %s ' % (meu_id, ordem_entrega))

    for p in outros:
        saida[p].join()
    time.sleep(0.5)


main()