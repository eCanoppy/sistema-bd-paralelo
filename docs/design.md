# Design — Sistema de Processamento Paralelo de Requisições a um Banco de Dados

Disciplina: Sistemas Operacionais — Avaliação M1 (IPC, Threads e Paralelismo) — Univali.

## Objetivo

Simular um gerenciador de banco de dados com dois processos de SO
separados (cliente e servidor), comunicação via IPC real (FIFO POSIX)
e processamento concorrente das requisições no servidor via pool de
threads, com exclusão mútua real (`pthread_mutex_t`) sobre a tabela
compartilhada.

## Ambiente

- Linguagem: C (C11), POSIX (pthreads, FIFOs `mkfifo`).
- Build/execução: WSL2 Ubuntu (gcc 15.2). Não roda em Windows nativo.
- Sem dependências externas além de libc/pthread.

## Componentes

### 1. `protocolo.h`

Formato de mensagem trocado nos FIFOs, texto delimitado por `|`,
uma linha por requisição/resposta. Evita binário para manter o
protocolo legível durante a defesa.

Requisição: `<pid_cliente>|<seq>|<OP>|<id>|<nome>`
- `OP` ∈ {INSERT, SELECT, UPDATE, DELETE}
- `nome` vazio para SELECT/DELETE.
- `seq`: número sequencial por cliente, ecoado na resposta — permite
  ao cliente casar resposta com pedido se um dia houver mais de uma
  requisição em voo (hoje o cliente é síncrono, mas o campo já
  documenta a extensão natural).

Resposta: `<seq>|<STATUS>|<payload>`
- `STATUS` ∈ {OK, NOT_FOUND, ERROR}
- `payload`: registro formatado para SELECT, vazio nos demais.

### 2. `banco.c` / `banco.h`

- `Registro { int id; char nome[50]; }`, vetor de tamanho fixo
  (`MAX_REGISTROS`) em memória do processo servidor.
- Um `pthread_mutex_t` global protege todo acesso de leitura/escrita
  ao vetor (INSERT, SELECT, UPDATE, DELETE) — exclusão mútua simples
  e correta é priorizada sobre um esquema de leitores/escritores, que
  seria otimização prematura para o volume desta avaliação.
- `banco_carregar(path)` lê `banco.txt` na inicialização do servidor
  (se existir) para o vetor; `banco_persistir(path)` reescreve o
  arquivo inteiro. Persistência é acionada: (a) após cada operação de
  escrita (INSERT/UPDATE/DELETE) para simplicidade e para que o
  arquivo sempre reflita o estado atual mesmo se o servidor for
  encerrado abruptamente, e (b) explicitamente no shutdown limpo.
  Dado o volume de teste (dezenas de operações), reescrever o arquivo
  inteiro a cada escrita é aceitável e evita um segundo mecanismo de
  append-log só para esta avaliação.

### 3. `servidor.c`

- Cria/abre o FIFO fixo de requisições (`bd_requisicoes.fifo`, path
  configurável, default no diretório de execução).
- Thread principal faz `open()` bloqueante do FIFO em modo leitura e
  fica lendo linhas; cada linha parseada vira uma tarefa enfileirada
  numa fila (array circular + `pthread_mutex_t` + `pthread_cond_t`).
- Pool fixo de N worker threads (default N=4, `pthread_create` na
  inicialização) consome da fila (`pthread_cond_wait`), executa a
  operação sobre o banco (seção protegida pelo mutex do banco) e
  escreve a resposta no FIFO de resposta específico do cliente
  (`bd_resposta_<pid>.fifo`, aberto em modo escrita — o cliente já
  deve estar com o seu lado em modo leitura aberto, ou o `open()`
  bloqueia até ele abrir).
- `SIGINT`/`SIGTERM`: handler sinaliza flag global; loop principal
  sai do `open`/leitura, sinaliza as workers para drenarem a fila e
  encerrarem, faz `banco_persistir` final, remove o FIFO de
  requisições (`unlink`) e sai.

### 4. `cliente.c`

- Recebe a requisição via argumentos de linha de comando (um comando
  por execução — `./cliente INSERT 7 Joao`, `./cliente SELECT 5`,
  etc.), o que permite tanto uso manual quanto disparo em massa por
  script de teste (`testes/teste_concorrencia.sh`).
- Cria seu próprio FIFO de resposta `bd_resposta_<pid próprio>.fifo`
  antes de enviar a requisição, evitando corrida entre "servidor tenta
  responder" e "cliente ainda não criou o FIFO".
- Escreve a requisição no FIFO de requisições do servidor (`open` em
  modo escrita — falha com mensagem clara se o servidor não estiver
  rodando, em vez de travar indefinidamente: usa `O_NONBLOCK` no
  `open` de escrita e trata `ENXIO`).
- Lê a resposta do seu próprio FIFO, imprime resultado formatado,
  remove seu FIFO de resposta (`unlink`) e sai.

## Fluxo de dados

```
cliente (proc A) --escreve--> bd_requisicoes.fifo --lê--> [thread leitora servidor]
                                                                  |
                                                     enfileira (mutex+cond)
                                                                  |
                                                          [pool de workers]
                                                                  |
                                                    mutex banco -> vetor Registro[]
                                                                  |
                                                          banco.txt (persist)
                                                                  |
cliente (proc A) <--lê-- bd_resposta_<pidA>.fifo <--escreve-- worker
```

## Tratamento de erros

- Servidor não encontra FIFO de resposta do cliente (cliente morreu
  antes de ler): `open` com timeout curto via `O_NONBLOCK` + retry
  limitado; se falhar, worker loga e descarta, sem travar as demais.
- Linha malformada no FIFO de requisições: parser retorna erro,
  servidor responde `ERROR` em vez de crashar.
- `banco.txt` ausente na primeira execução: servidor inicia com banco
  vazio, sem erro.

## Teste de concorrência (evidência para o relatório)

`testes/teste_concorrencia.sh`: sobe o servidor em background, dispara
K clientes em paralelo (`&` + `wait`) cada um com um INSERT de ID
único, espera todos terminarem, depois um SELECT sequencial de cada ID
inserido para confirmar que nenhum registro foi perdido/corrompido, e
mede o tempo total para K variando (ex. 1, 4, 8, 16 threads no pool)
— vira a tabela/gráfico de throughput por nº de threads no relatório.

## Fora de escopo (YAGNI)

- Sem esquema de leitores/escritores (rwlock) — mutex simples é
  suficiente e mais fácil de defender.
- Sem SQL real nem SQLite — vetor + arquivo texto atende ao enunciado
  ("pode ser um vetor ou um arquivo") com menos superfície de bugs.
- Sem múltiplas requisições em voo por cliente (cliente é
  sincrono/bloqueante por design — um processo cliente = uma
  requisição = uma resposta = sai).
