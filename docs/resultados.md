# Resultados medidos — throughput e integridade sob concorrência

Coletado em 22/09/2026 via `testes/coletar_resultados.sh`, que roda
`testes/teste_concorrencia.sh <K> <threads>` para cada combinação de
tamanho de pool de threads do servidor e número de clientes disparados
em paralelo. Cada linha da tabela é uma execução completa: sobe o
servidor, dispara K processos-cliente simultâneos fazendo `INSERT`,
espera todos terminarem, e então confirma via `SELECT` que todos os K
registros estão íntegros (nenhum perdido/corrompido).

## Achado durante a coleta: bug de concorrência real encontrado e corrigido

A primeira rodada da matriz (antes da correção abaixo) corrompeu
requisições a partir de ~90 clientes simultâneos: o servidor descartava
parte de uma mensagem no meio quando várias requisições concorrentes
chegavam coladas num único `read()` do FIFO e o buffer de remontagem de
linha (256 bytes, pensado para o tamanho de *uma* linha) estourava ao
tentar acumular *várias* linhas ainda não processadas. A correção
(commit `fix: enlarge servidor line-reassembly buffer to survive
concurrent bursts`) aumentou esse buffer de acumulação para 8 KB,
dimensionado pelo volume esperado de mensagens em trânsito, não pelo
tamanho de uma mensagem individual. Após a correção, todas as 12
combinações abaixo rodaram sem nenhuma mensagem descartada.

Esse achado é relevante para a discussão do trabalho: mostra que a
seção crítica protegida por mutex (`banco_inserir`) nunca corrompeu
dado em nenhuma rodada, em nenhum nível de concorrência — o bug estava
na camada de IPC (reconstrução de mensagens do FIFO), não na exclusão
mútua sobre a tabela compartilhada.

## Tabela de resultados (pós-correção)

| threads no pool | K (clientes concorrentes) | tempo total (s) | integridade |
|---:|---:|---:|:---|
| 1 | 10  | 0.034 | íntegro |
| 1 | 50  | 0.030 | íntegro |
| 1 | 100 | 0.059 | íntegro |
| 2 | 10  | 0.014 | íntegro |
| 2 | 50  | 0.031 | íntegro |
| 2 | 100 | 0.053 | íntegro |
| 4 | 10  | 0.014 | íntegro |
| 4 | 50  | 0.029 | íntegro |
| 4 | 100 | 0.054 | íntegro |
| 8 | 10  | 0.015 | íntegro |
| 8 | 50  | 0.030 | íntegro |
| 8 | 100 | 0.053 | íntegro |

## Leitura rápida dos números

- **Integridade: 12/12 íntegro**, incluindo o cenário mais pesado (8
  threads, 100 clientes concorrentes) — o mutex sobre o vetor
  compartilhado se sustenta sob a carga testada.
- **Tempo não cai de forma proporcional ao número de threads** (1→8
  threads não reduz o tempo com 100 clientes: ~0.054–0.059s em todos os
  casos). Isso é esperado, não é sinal de bug: com K processos-cliente
  de vida curta, o custo dominante é o *fork/exec* de cada processo
  cliente e a ida-e-volta pelo FIFO, não o tempo gasto dentro da seção
  crítica (uma escrita num vetor em memória, que dura microssegundos).
  O gargalo real deste desenho é o modelo "um processo por
  requisição" do lado cliente, não o tamanho do pool de threads do
  servidor — é exatamente esse tipo de leitura que a análise de
  desempenho concorrente deve produzir, em vez de assumir que mais
  threads sempre significa mais rápido.
