# Decisões de arquitetura

## Organização

- `domain/` contém `Money`, `Wallet`, `WagerTransaction`, `WalletLedgerEntry` e eventos. Essas regras não dependem de NestJS nem de TypeORM.
- `application/use-cases/` concentra o processamento financeiro, consultas, criação de wallet e reconciliação.
- `infrastructure/persistence/` contém o DataSource TypeORM, SQL explícito e migrations reversíveis.
- `infrastructure/messaging/`, `outbox/` e `workers/` integram SQS e executam os processos de fundo.
- `presentation/http/` valida DTOs e traduz chamadas HTTP para os casos de uso.

O TypeORM é usado para conexão, ciclo de vida e migrations; SQL explícito torna visíveis as transações, constraints e locks que protegem o dinheiro.

## Dinheiro e transação financeira

`Money` guarda unidades mínimas como `bigint` e recebe/serializa valores como strings decimais. Assim, não há conversão monetária para ponto flutuante. O schema persiste valores em `numeric(20,2)` e a aplicação os reidrata a partir de texto. O domínio limita os valores à faixa desse tipo para rejeitar entradas que o banco não conseguiria armazenar.

O processamento de uma transação roda em `READ COMMITTED`. O caso de uso abre uma transação SQL, registra (quando aplicável) a inbox, verifica idempotência, bloqueia a wallet, insere a transação e aplica a regra. Wallet, ledger, snapshot idempotente, inbox e eventos de outbox são confirmados no mesmo commit. Erros transitórios revertem tudo.

Wallets iniciam na versão 1. A versão só é atualizada quando o saldo muda. Um saldo inicial positivo produz uma transação interna `OPENING` e o primeiro crédito de ledger na mesma transação que cria a wallet.

## Concorrência e invariantes no banco

A unidade de lock é uma wallet. O processador executa `SELECT ... FOR UPDATE` na linha da wallet antes de calcular/aplicar o próximo saldo. O worker de referências segue a mesma ordem (wallet e depois transação pendente), evitando inversão de locks com operações normais. Operações da mesma wallet ficam serializadas; wallets diferentes podem avançar em paralelo. Instâncias e processos não compartilham lock em memória.

Constraints e índices únicos persistentes protegem `(player_id, currency)`, `(provider_id, idempotency_key)`, `(provider_id, external_transaction_id)`, uma entrada de ledger por transação/wallet e uma reversão resolvida por referência/tipo. Chaves estrangeiras compostas impedem referências fora do escopo de provedor, jogador, wallet, rodada e moeda. O schema impede saldo e lançamento negativos e valida a equação do ledger. Triggers rejeitam `UPDATE`, `DELETE` e `TRUNCATE` no ledger.

O ledger guarda `balance_before` e `balance_after`; a reconciliação compara o saldo persistido à soma de créditos e débitos em uma única consulta, para que ambos venham do mesmo snapshot MVCC durante movimentações concorrentes. Divergências são devolvidas e contabilizadas, nunca corrigidas silenciosamente.

## Idempotência e referências

A chave de idempotência é persistida e tem escopo por provedor. O hash SHA-256 usa JSON canônico com chaves ordenadas dos campos de negócio; chave e metadados de transporte ficam fora do hash. Repetições idênticas retornam o snapshot original. Reutilizar uma chave com payload diferente é conflito `409`.

O consumidor de SQS passa por `ProcessWagerTransaction`, o mesmo caso de uso do HTTP. A tabela inbox usa a chave `(consumer_name, message_id)`. O ACK ocorre após o commit. Em caso de falha transitória, a mensagem permanece na fila e o consumidor altera a visibilidade com backoff de `2^receiveCount` segundos, limitado a 300 segundos. A redrive policy tem `maxReceiveCount=5`. Mensagens inválidas/permanentemente incompatíveis são copiadas para a DLQ com metadados de classificação antes do ACK da original. No encerramento, mensagens recebidas no mesmo lote e ainda não iniciadas têm a visibilidade zerada para redelivery imediato.

O `DeadLetterQueueConsumer` registra mensagens recebidas da DLQ em `dead_letter_messages` e só as confirma depois do commit PostgreSQL. A chave `(source_queue, message_id)` e o upsert condicionado ao hash do corpo tornam a repetição segura; um ID reutilizado com outro corpo não é confirmado. Falha de banco ou do ACK deixa a mensagem para nova entrega. O registro distingue `INVALID_MESSAGE`, `PERMANENT_FAILURE`, `RETRY_EXHAUSTED` e `UNKNOWN`. O payload armazenado é uma projeção limitada aos campos conhecidos de apostas válidas; corpos inválidos ficam identificados pelo hash, sem cópia em logs.

O estado financeiro `FAILED` foi removido do domínio e do schema por migration aditiva. Não havia transações persistidas nesse estado; a migration falha com erro explícito se encontrar alguma. Falhas técnicas transitórias revertem a transação financeira e são retentadas pelo SQS. Rejeições de negócio ficam `REJECTED`, e falhas operacionais esgotadas são registradas na DLQ. Isso evita marcar como `FAILED` uma operação que ainda pode ser recuperada por redelivery ou gravar um estado técnico em transação separada do efeito financeiro.

A triagem e o replay são operações locais da CLI `bun run dead-letter`. `list` retorna metadados, sem payload. `prepare` bloqueia a linha, valida uma mensagem `RETRY_EXHAUSTED` e verifica se já existe transação financeira correspondente; mensagens inválidas/permanentes não podem ser reproduzidas. `replay` grava o corpo original e seu hash em `dead_letter_replay_outbox` e muda o registro para `REPLAY_QUEUED` no mesmo commit. O publisher reclama linhas com `SKIP LOCKED`, lease e retry persistidos; publica novamente o messageId original em `wager-transactions.fifo`. O consumidor atualiza o status operacional após processar o evento financeiro. O replay não é automático e não cria uma operação financeira nova para contornar a idempotência. Sem autenticação HTTP administrativa, a CLI é restrita pela política operacional de acesso à máquina/ambiente.

Se o processo morrer depois do envio à SQS e antes de gravar `sent_at`, o lease expira e o publisher pode reenviar o evento. A outbox de replay é portanto durável e `at-least-once`; a inbox e a chave idempotente financeira protegem os efeitos. O evento reaproveita o identificador original e a FIFO pode suprimir duplicatas dentro da janela configurada, mas não é garantia permanente.

A retenção de `dead_letter_messages` usa como padrão 90 dias, lote 500 e intervalo de 24 horas, configuráveis por `DEAD_LETTER_RETENTION_DAYS`, `DEAD_LETTER_RETENTION_BATCH_SIZE` e `DEAD_LETTER_RETENTION_INTERVAL_MS`. Um advisory lock impede rotinas simultâneas. Apenas ocorrências `RESOLVED`/`REPROCESSED` vencidas podem ser removidas; a idade compara resolução, última entrega e timestamps financeiros conhecidos. A rotina protege replays ainda não enviados e transações `PENDING`/`PENDING_REFERENCE`. Ela nunca apaga histórico financeiro. Essa política operacional é configurável, não representa prazo legal de auditoria.

`REFUND` e `ROLLBACK` sem referência são armazenados como `PENDING_REFERENCE`. Um worker agenda tentativas a partir de cinco segundos, com backoff exponencial limitado a cinco minutos e até dez verificações. Ao esgotar as tentativas, a transação é rejeitada com `REFERENCE_NOT_FOUND`. Referências inválidas e duplicadas também terminam como rejeição auditável.

## Outbox e publicação

Eventos são persistidos junto com a transação financeira e só publicados depois do commit. Cada publisher reclama lotes com `FOR UPDATE SKIP LOCKED`, registra um lease de 60 segundos, publica na fila FIFO de eventos e marca o registro como publicado. Uma sequência persistente do banco mantém a ordem dos eventos da mesma wallet; um evento só fica elegível quando eventos anteriores da wallet já foram publicados. Wallets diferentes continuam em paralelo. Falhas agendam retry exponencial até cinco minutos; leases vencidos permitem recuperação após encerramento do processo.

A publicação tem semântica `at-least-once`: se o processo morrer ou ocorrer um erro após publicar e antes de marcar a outbox, o evento pode ser enviado novamente. Cada evento mantém um `eventId` estável usado como `MessageDeduplicationId` FIFO. A integração injeta uma falha nessa fronteira e comprova que as duas tentativas usam o mesmo ID; o LocalStack mostra uma mensagem visível dentro da janela de deduplicação. Isso não prova deduplicação permanente.

O projeto não consome `wager-events.fifo`. Não existe um efeito downstream interno definido, e um consumer neste mesmo queue competiria com os consumidores externos, retirando mensagens que pertencem a eles. O contrato publicado contém `eventId`, `eventType`, `occurredAt`, `aggregateId`, `sequence`, `transactionId`, `walletId`, `providerId`, `externalTransactionId`, `status`, `failureCode`, `money` e `walletVersion`.

Cada consumidor downstream deve tratar `eventId` como identificador idempotente: inserir uma receipt com `event_id` único e aplicar seu efeito local dentro da mesma transação PostgreSQL. Se a receipt já existir, confirmar a mensagem sem repetir o efeito. Se a aplicação do efeito falhar, a receipt e o efeito devem sofrer rollback juntos, permitindo retry. Um teste de integração usa um evento real de `wager-events.fifo` e tabelas temporárias PostgreSQL para demonstrar a sequência deduplicação/rollback; esse teste é um exemplo de contrato, não um consumidor instalado na aplicação. Se o efeito ocorrer em sistema externo, não há transação atômica compartilhada com PostgreSQL: use idempotency key/consulta idempotente do provedor externo e reconciliação antes de confirmar, ou mantenha uma outbox local para a chamada, reconhecendo a possibilidade de entrega repetida. A FIFO recebe `MessageGroupId=walletId` e `MessageDeduplicationId=eventId`; isso reduz duplicatas na janela da SQS, mas a publicação permanece `at-least-once`.

## Operações e observabilidade

Docker Compose inicia PostgreSQL, LocalStack, API e worker. Os dois processos com workers ativos podem consumir em paralelo; as constraints e locks do banco sustentam a correção. A fila de transações é FIFO por `walletId` para ajudar na ordenação, mas não é a garantia de consistência.

Logs JSON incluem IDs operacionais e códigos de erro sem registrar payloads financeiros completos. Prometheus expõe contadores por status, duplicatas, retries, DLQ, conflitos do banco, latência, lag da outbox e divergências de reconciliação. `/health/live` verifica o processo; `/health/ready` verifica PostgreSQL e as filas SQS.

As métricas são locais a cada processo; um coletor deve raspar tanto a API (`3000/metrics`) quanto o worker (`9464/metrics`) para agregar a visão dos dois. A checagem de saúde do LocalStack exige que as filas de transações, DLQ e eventos existam; a suíte verifica `VisibilityTimeout=60` e `maxReceiveCount=5` na fila de transações. O teste funcional de redrive reduz a visibilidade somente na fila isolada de teste. A validação temporal separada (`RUN_PRODUCTION_REDRIVE_TESTS=true bun run test:integration`) também passou no LocalStack sem sobrescrever atributos: observou cinco recebimentos, encaminhamento efetivo para DLQ e gravação operacional. Ela levou cerca de 67 segundos; comportamento e timing ainda dependem da implementação do LocalStack.

## Autenticação

Autenticação foi deixada fora do escopo porque o desafio não atribui pontos a ela. A classe abstrata `ProviderIdentityPort` é o ponto de integração para um IdP/OIDC ou validação de credenciais de provedor. Atualmente, essa porta não está ligada a um guard: endpoints de negócio não autenticam chamadas. Health fica aberto, e SQS é tratado como canal interno confiável, conforme permitido pelo enunciado.

## Falhas, trade-offs e limitações

- A solução prioriza invariantes explícitas no PostgreSQL e SQL direto em vez de abstrações de repositório extensas. Isso deixa o fluxo financeiro mais fácil de auditar, embora acople as queries ao PostgreSQL.
- `NODE_ENV=production` exige URLs, endpoints, região, credenciais e filas configurados explicitamente; mensagens de inicialização mostram o nome da variável ausente sem imprimir valores sensíveis. Em desenvolvimento, há defaults locais para Compose/LocalStack.
- A serialização por wallet favorece simplicidade e correção para uma wallet quente; alta disputa pode aumentar latência. Conflitos/retryables são contabilizados e retornados como erro transitório quando necessário.
- O outbox não garante publicação única, e consumidores downstream precisam ser idempotentes.
- **Comprovado por testes:** em 2026-10-09, a suíte de integração com PostgreSQL 16 e LocalStack 4.14 cobre o saldo concorrente, 50 submissões idempotentes em três processos, replay, constraints, rollback atômico, reconciliação, `LOSS`, `REFUND`/`ROLLBACK`, referências fora de ordem e publishers concorrentes. Também encerra um processo HTTP depois do commit e antes da resposta, repete a requisição em outro processo, encerra um worker depois do commit e antes do ACK, e reprocessa a redelivery com um segundo worker. A inbox/ledger/outbox permanecem únicos, e o saldo final é o esperado.
- **Comprovado por testes:** o `WagerInboxConsumer` real trata falha no ACK e recebe a mesma mensagem outra vez; a inbox persistente impede repetir o débito. O consumer da DLQ persiste uma mensagem inválida já colocada na DLQ; um cenário separado comprova que a inbox roteia um conflito permanente para lá. Uma falha SQL após o insert operacional faz rollback e não confirma a mensagem; uma falha depois do commit e antes do ACK gera redelivery sem duplicar a linha. Cinco falhas transitórias reais levam o LocalStack à DLQ, onde o registro classifica `RETRY_EXHAUSTED`; a aposta não fica em `FAILED` nem altera saldo, ledger ou inbox. O harness zera visibilidade para acelerar o cenário; os atrasos de produção não são medidos por esse teste.
- **Comprovado por testes:** uma falha injetada após a SQS aceitar o evento e antes do publisher gravar `published_at` deixa a outbox recuperável. As duas tentativas usam o mesmo `eventId`; o LocalStack mantém uma cópia visível na janela FIFO de deduplicação e o estado financeiro continua com um único débito.
- **Derivado da implementação, não de cenário específico:** atomicidade SQL, constraints, locking da wallet, leases de outbox, retry/backoff e snapshot persistido dão as garantias descritas nas seções anteriores. O teste de falha cobre um erro de SQL controlado e o de outbox cobre a fronteira entre envio e marcação, mas não indisponibilidade prolongada dos serviços nem falha de hardware.
- **Limitações conhecidas:** a CLI DLQ depende de acesso operacional ao ambiente e ao banco; não há autenticação HTTP nem painel administrativo. O replay só é permitido para envelopes válidos classificados como `RETRY_EXHAUSTED`; corrigir payloads inválidos demanda procedimento fora do replay automático, para evitar inventar ou alterar dados de negócio. Não há consumidor downstream implantado, pois o projeto não define um efeito interno e a fila é compartilhada. O teste downstream demonstra um padrão em tabelas temporárias, não garante comportamento de sistemas externos. A deduplicação FIFO tem janela limitada. Autenticação de endpoints de negócio permanece fora do escopo autorizado pelo desafio.
- Os testes de integração são serializados para evitar interferência entre fixtures independentes; concorrência financeira e publishers paralelos permanecem simultâneos nos respectivos casos.
