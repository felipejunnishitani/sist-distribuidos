# Atividade 4 - Sistemas Distribuidos
# Eleicao de lider - Algoritmo do Fracao (menor ID vence)
# Felipe Jun Nishitani - 822353
# Gabriel Araujo Streicher - 822485
#
# USO: py eleicao.py <meu_id> [n_processos]   (n_processos padrao = 5)


import socket
import threading
import queue
import json
import time
import sys
import uuid

HOST = '127.0.0.1'
PORTA_BASE = 5000
N = 5 # numero de processos
HEARTBEAT = 1.0
FALHA = 4.0
RESPOSTA = 2.0
ANUNCIO = 4.0

#####################
#### argumentos #####
#####################
def le_args():
    try:
        if len(sys.argv) not in (2, 3):
            raise ValueError
        ident = int(sys.argv[1])
        n = int(sys.argv[2]) if len(sys.argv) == 3 else N
        if n < 2 or not (1 <= ident <= n):
            raise ValueError
    except ValueError:
        print('uso: py eleicao.py <meu_id 1..n> [n_processos, padrao %d]' % N)
        sys.exit(1)
    return ident, n


#####################
##### estado ########
#####################
meu_id = 0
outros = []
sessao = uuid.uuid4().hex  # muda quando este processo reinicia
inicio = time.monotonic()
ativos = {}
sessoes = {}
antigas = set()
lider = None
estado = 'NORMAL' # NORMAL, RESPOSTAS ou ANUNCIO
rodada = 0
prazo = 0.0
recebidas = queue.Queue()
saida = {}


def log(texto):
    print('t=%5.2f p%d | %s' % (time.monotonic() - inicio, meu_id, texto),
          flush=True)


#####################
######## rede #######
#####################
def abre_porta():
    s = socket.socket()
    if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
        s.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
    else:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind((HOST, PORTA_BASE + meu_id))
        s.listen(N)
    except OSError as erro:
        s.close()
        print('Nao abriu a porta %d. Outro p%d rodando? %s'
              % (PORTA_BASE + meu_id, meu_id, erro))
        sys.exit(1)
    return s


def escuta(s):
    while True:
        conn, _ = s.accept()
        threading.Thread(target=le, args=(conn,), daemon=True).start()


def le(conn):
    try:
        with conn, conn.makefile('r', encoding='utf-8') as arq:
            for linha in arq:
                recebidas.put(json.loads(linha))
    except (OSError, ValueError):
        pass


def envia_loop(p):
    s = None
    while True:
        msg = saida[p].get()
        try:
            if s is None:
                s = socket.create_connection((HOST, PORTA_BASE + p), timeout=0.3)
            s.sendall((json.dumps(msg) + '\n').encode('utf-8'))
        except OSError:
            if s is not None:
                s.close()
            s = None # tenta reconectar no proximo envio, sem guardar o antigo
        finally:
            saida[p].task_done()


def envia(p, tipo, **dados):
    msg = {'tipo': tipo, 'de': meu_id, 'sessao': sessao}
    msg.update(dados)
    try:
        saida[p].put_nowait(msg)
    except queue.Full:
        pass # nao acumular mensagens para um destino indisponivel


def todos(tipo, **dados):
    for p in outros:
        envia(p, tipo, **dados)


#####################
##### algoritmo #####
#####################
def sou_lider():
    global lider, estado
    lider = meu_id
    estado = 'NORMAL'
    log('SOU LIDER')
    todos('LIDER')


def eleicao(motivo):
    global rodada, prazo, estado, lider
    if estado != 'NORMAL':
        return
    rodada += 1
    lider = None
    estado = 'RESPOSTAS'
    prazo = time.monotonic() + RESPOSTA
    menores = list(range(1, meu_id))
    log('ELEICAO %d: %s; consultando %s' % (rodada, motivo, menores))
    for p in menores:
        envia(p, 'ELEICAO', rodada=rodada)
    if not menores:
        sou_lider()


def reconhece_lider(p):
    global lider, estado
    if p > meu_id:
        if lider == meu_id:
            todos('LIDER')
        else:
            eleicao('anuncio de ID maior que o meu')
        return
    if lider is not None and lider < p and (lider == meu_id or lider in ativos):
        return
    if lider != p or estado != 'NORMAL':
        log('LIDER reconhecido: p%d' % p)
    lider = p
    estado = 'NORMAL'


def trata(msg):
    global estado, prazo, lider
    p, execucao = msg['de'], msg['sessao']
    if p not in outros or (p, execucao) in antigas:
        return
    voltou = p not in ativos or sessoes.get(p) != execucao
    if sessoes.get(p) is not None and sessoes[p] != execucao:
        antigas.add((p, sessoes[p]))
        if lider == p:
            lider = None
    sessoes[p] = execucao
    ativos[p] = time.monotonic()
    if voltou:
        log('ENTROU/REENTROU p%d' % p)

    tipo = msg['tipo']
    if tipo == 'ELEICAO' and meu_id < p:
        envia(p, 'OK', rodada=msg['rodada'], para_sessao=execucao)
        log('OK para p%d, rodada %d' % (p, msg['rodada']))
        if lider == meu_id:
            todos('LIDER')
        else:
            eleicao('convocado por p%d' % p)
    elif tipo == 'OK':
        if (estado == 'RESPOSTAS' and p < meu_id
                and msg['para_sessao'] == sessao and msg['rodada'] == rodada):
            estado = 'ANUNCIO'
            prazo = time.monotonic() + ANUNCIO
            log('OK de p%d; esperando anuncio por %.0fs' % (p, ANUNCIO))
        else:
            log('OK ignorado (fora da rodada ou espera atual)')
    elif tipo == 'LIDER':
        reconhece_lider(p)
    elif tipo == 'HEARTBEAT' and msg['lider'] == p:
        reconhece_lider(p)

    if voltou and lider is not None and p < lider:
        eleicao('voltou um ID menor que o lider')


def verifica_tempos():
    global lider, estado
    agora = time.monotonic()
    for p in list(ativos):
        if agora - ativos[p] >= FALHA:
            del ativos[p]
            log('FALHA: p%d removido dos ativos (sem noticias)' % p)
            if lider == p:
                lider = None

    if estado == 'RESPOSTAS' and agora >= prazo: # esperando OK
        log('TIMEOUT: nenhum menor respondeu')
        sou_lider()
    elif estado == 'ANUNCIO' and agora >= prazo: # OK recebido, esperando anuncio do lider
        log('TIMEOUT: candidato nao anunciou lider')
        estado = 'NORMAL'
        eleicao('anuncio nao chegou')
    elif estado == 'NORMAL' and lider is None: # nenhum lider ativo
        eleicao('sem lider ativo')


#####################
######## main #######
#####################
def main():
    global meu_id, outros, saida, N
    meu_id, N = le_args()
    outros = [p for p in range(1, N + 1) if p != meu_id]
    saida = {p: queue.Queue(maxsize=20) for p in outros}
    s = abre_porta()
    threading.Thread(target=escuta, args=(s,), daemon=True).start()
    for p in outros:
        threading.Thread(target=envia_loop, args=(p,), daemon=True).start()
    log('iniciado; Ctrl+C derruba este processo')
    eleicao('partida')
    proximo_hb = proximo_status = 0.0
    try:
        while True:
            try:
                trata(recebidas.get(timeout=0.1))
            except queue.Empty:
                pass
            verifica_tempos()
            agora = time.monotonic()
            if agora >= proximo_hb:
                todos('HEARTBEAT', lider=lider)
                proximo_hb = agora + HEARTBEAT
            if agora >= proximo_status:
                log('ATIVOS=%s LIDER=%s ESTADO=%s'
                    % (sorted([meu_id] + list(ativos)), lider, estado))
                proximo_status = agora + 3.0
    except KeyboardInterrupt:
        log('encerrado por Ctrl+C')
    finally:
        s.close()


if __name__ == '__main__':
    main()
