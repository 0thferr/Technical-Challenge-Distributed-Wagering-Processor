# Distributed Wagering Processor

Backend de processamento financeiro para transações de apostas. A aplicação recebe operações de provedores via HTTP ou SQS, controla o saldo de cada wallet no PostgreSQL e registra todas as movimentações em um ledger imutável.

## Stack

- Bun 1.x, TypeScript strict e NestJS
- PostgreSQL 16 com TypeORM e migrations SQL versionadas
- AWS SQS via LocalStack
- Docker Compose
- Biome para lint e formatação; testes com Bun

## Pré-requisitos

Para executar localmente: Bun 1.x e Docker com Docker Compose. O Compose constrói a aplicação e inicia PostgreSQL, LocalStack, API e worker. As credenciais do Compose são apenas locais e não devem ser usadas fora deste ambiente.

## Iniciar

Na raiz do projeto:

```sh
docker compose up --build
```

O container da API aplica as migrations antes de iniciar. O endpoint HTTP fica em `http://localhost:3000`; o PostgreSQL fica em `localhost:5432` e o LocalStack em `localhost:4566`. O serviço `worker` executa um segundo consumidor/publisher para permitir concorrência entre processos. A API também executa workers no Compose.

Para parar os containers:

```sh
docker compose down
```

Para remover também os dados locais do PostgreSQL e LocalStack:

```sh
docker compose down -v
```

O arquivo [.env.example](.env.example) lista os valores locais esperados. Para executar a aplicação fora do Compose, copie-o para `.env` e inicie PostgreSQL e LocalStack separadamente. Bun e Docker Compose carregam `.env` automaticamente; `.env.example` é apenas um modelo e não é carregado diretamente.

## Migrations

Com o banco acessível e as variáveis de ambiente configuradas:

```sh
bun run migration:run
bun run migration:show
bun run migration:down
```

`migration:down` reverte a última migration e pode remover dados operacionais. As migrations aditivas criam o registro de DLQ, os estados de triagem e a outbox de replay. A migration mais recente remove `FAILED` do estado financeiro após verificar que não há transações persistidas nesse estado; seu `down` se recusa a remover registros de replay ou estados operacionais que seriam perdidos.

## API

### Criar wallet

```sh
curl -X POST http://localhost:3000/wallets \
  -H 'content-type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'
```

Um saldo inicial positivo cria uma transação interna `OPENING` e um crédito no ledger dentro da mesma transação SQL.

### Enviar aposta

```sh
curl -X POST http://localhost:3000/wagering/transactions \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"<wallet-uuid-retornado>","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

O `Idempotency-Key` é obrigatório. A mesma chave e o mesmo payload retornam o resultado original; a mesma chave com payload diferente retorna `409 IDEMPOTENCY_KEY_PAYLOAD_MISMATCH`. Valores monetários são strings decimais com até duas casas, e `OPENING` não pode ser enviado por este endpoint.

`POST /wagering/transactions/async` responde `202` quando o SQS aceita a mensagem. A validação persistente de idempotência ocorre no consumidor; por isso, essa rota informa aceite para processamento, não o resultado financeiro nem um conflito já resolvido.

### Outras rotas

| Método e rota | Uso |
|---|---|
| `GET /wallets/:walletId` | Saldo e versão atuais |
| `GET /wallets/:walletId/ledger?cursor=...&limit=50` | Ledger com cursor opaco e estável |
| `POST /wallets/:walletId/reconciliation` | Compara saldo armazenado com a soma do ledger |
| `GET /wagering/transactions/:transactionId` | Consulta pelo ID interno |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | Consulta pelo ID do provedor |
| `POST /wagering/transactions/async` | Enfileira processamento e responde `202` |
| `GET /health/live` | Liveness |
| `GET /health/ready` | Verifica PostgreSQL e SQS |
| `GET /metrics` | Métricas Prometheus da API |

O worker expõe suas métricas em `/metrics` na porta interna `9464`. Respostas `REJECTED` do processamento síncrono usam HTTP `422`; referências pendentes usam `202`. Conflitos de idempotência usam `409`; falhas transitórias de infraestrutura usam `503` e incluem `retryable: true`. Um cursor de ledger inválido retorna `400`.

### Consultar logs e filas

```sh
docker compose logs -f app worker postgres localstack
docker compose exec localstack awslocal sqs list-queues
docker compose exec localstack awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/wager-transactions-dlq.fifo \
  --attribute-names ApproximateNumberOfMessages
```

Os logs são JSON estruturado. Eles registram IDs de correlação/transação/wallet/provedor e códigos de erro, sem registrar o payload financeiro completo.

## Desenvolvimento e validação

Com as dependências locais disponíveis:

```sh
bun install --frozen-lockfile
bun run dev
bun run typecheck
bun run lint
bun run format
bun run build
bun test
```

`bun test` executa os testes unitários e pula a suíte de integração, a menos que `RUN_INTEGRATION_TESTS=true` esteja definido. O comando dedicado inicia PostgreSQL e LocalStack reais via Compose e então roda os testes de integração:

```sh
bun run test:integration
```

A suíte cobre duas apostas concorrentes contra o mesmo saldo, 50 entregas simultâneas com a mesma chave, 50 submissões em três processos Bun independentes, replay com snapshot original, refund fora de ordem pelo worker e invariantes de reconciliação. Com PostgreSQL e LocalStack reais, também encerra e reinicia processos HTTP e worker após o commit, percorre o `WagerInboxConsumer` na redelivery, força uma falha transitória até a mensagem chegar à DLQ, verifica o registro de mensagens inválidas já na DLQ e o roteamento de conflitos permanentes pela inbox, injeta falhas na persistência/ACK do registro operacional da DLQ e depois de a SQS aceitar um evento da outbox. Cada caso verifica o estado persistido além das respostas. A suíte requer Bun compatível e um daemon Docker ativo. Os casos são executados serialmente; a concorrência permanece real dentro dos cenários que precisam dela.

`bun run test:integration` mapeia PostgreSQL em `localhost:55432` e LocalStack em `localhost:54566`, para não disputar as portas padrão `5432` e `4566` usadas pelo ambiente de desenvolvimento. O script cria as filas e aplica as migrations durante a preparação dos testes; os containers permanecem ativos ao terminar e podem ser parados com `docker compose down`.

## Garantias e limites validados

- **Comprovado na integração:** a repetição após reinício HTTP retorna o snapshot do primeiro commit; uma única transação, débito, mudança de versão e par de eventos permanecem no banco. O worker SQS real também é encerrado entre commit e ACK, reiniciado e deixa a fila vazia sem repetir o débito. A falha de ACK dentro do consumidor real produz duas entregas, mas só um efeito financeiro.
- **Comprovado na integração:** cinco entregas reais falham por uma falha SQL injetada; o polling posterior do broker faz o LocalStack aplicar a política `maxReceiveCount=5`, e a mensagem é recebida na DLQ. O consumer real registra uma mensagem inválida já encaminhada à DLQ; outro cenário confirma que a inbox encaminha um conflito permanente antes de confirmar a mensagem original.
- **Comprovado na integração:** o consumer real da DLQ grava a mensagem e a classificação no PostgreSQL antes do ACK. Uma falha após a tentativa de insert não cria registro nem remove a mensagem; uma nova entrega consegue gravar. Uma falha depois do commit e antes do ACK redelivera a mensagem, mas mantém uma única linha idempotente. Mensagens inválidas, conflitos permanentes e esgotamento de redrive recebem classificações diferentes.
- **Comprovado na integração:** após o `SendMessage` da outbox ser aceito, uma falha é injetada antes de `published_at`. Uma nova tentativa publica o mesmo `eventId`; a fila FIFO mostra uma cópia dentro da janela de deduplicação do SQS e o banco conserva uma única operação financeira.
- **Derivado da implementação:** inbox, transação, saldo, ledger e outbox compartilham o commit SQL. O consumer só solicita ACK depois de o caso de uso concluir; exceções transitórias deixam a mensagem sem ACK e aplicam backoff de visibilidade. O consumer da DLQ grava o registro operacional por upsert único antes de confirmar a mensagem. O banco protege idempotência e unicidade entre instâncias.
- **Limites:** o outbox é `at-least-once`. A deduplicação FIFO do SQS não é permanente nem substitui idempotência persistente de consumidores downstream. O projeto publica `wager-events.fifo`, mas não consome essa mesma fila: um consumer local competiria com os consumidores externos e poderia retirar mensagens deles. Cada consumidor downstream deve persistir `eventId` com unicidade e aplicar seu efeito no mesmo banco/transação.
- **Comprovado na integração:** replay controlado de mensagem `RETRY_EXHAUSTED` registra a mensagem da DLQ, exige preparação explícita e agenda o reenvio em uma outbox PostgreSQL. O agendamento e o publisher são executados em processos diferentes; o consumidor real aceita a reentrega, e o saldo, ledger, inbox e transação permanecem sem duplicação. A retenção remove um registro resolvido expirado em lote e preserva os recentes, pendentes, em replay e associados a uma referência financeira ainda pendente.
- **Comprovado na validação temporal separada:** com a fila de produção do ambiente de teste em `VisibilityTimeout=60` e `maxReceiveCount=5`, cinco recebimentos reais falharam e o LocalStack encaminhou a mensagem à DLQ, onde o consumer persistiu `RETRY_EXHAUSTED`. O teste levou cerca de 67 segundos, não alterou os atributos da fila e passou junto com os outros 25 testes de integração.
- **Derivado da implementação:** o replay mantém o messageId original, registra o hash do corpo reapresentado e usa a mesma chave de idempotência financeira; o outbox administrativo usa lease e retry persistidos. Uma falha após enviar à SQS pode produzir outra publicação após expirar o lease. A deduplicação financeira/inbox é persistente; FIFO é apenas uma ajuda temporária.
- **Limites:** `FAILED` foi removido do domínio e da constraint do schema porque nenhuma transação existente o usava e o fluxo não tinha uma transição atômica que diferenciasse uma falha permanente de uma falha recuperável. Falhas técnicas transitórias são revertidas para redelivery; rejeições financeiras permanecem `REJECTED`; falhas esgotadas são registradas operacionalmente na DLQ. Mensagens inválidas podem não formar uma transação financeira.
- **Limites:** a integração regular acelera o ciclo de redrive em um harness isolado. A validação do tempo real da política configurada pode ser executada separadamente com `RUN_PRODUCTION_REDRIVE_TESTS=true bun run test:integration`; ela depende do comportamento e do tempo de redrive do LocalStack e não altera os atributos de produção.

## Triagem, replay e retenção da DLQ

O worker persiste cada mensagem recebida da DLQ em `dead_letter_messages`, usando `(source_queue, message_id)` como chave e validando o hash do corpo em reentregas. A tabela guarda a classificação, código, contagens e timestamps. Envelopes conhecidos de `WagerTransactionRequested` são projetados para os campos de negócio necessários ao replay; mensagens inválidas ficam identificadas pelo ID e hash. Payloads não são escritos nos logs.

Consulte os itens abertos com:

```sql
SELECT source_queue, message_id, failure_type, failure_code, status,
       delivery_count, sqs_receive_count, body_hash, first_received_at,
       last_received_at, resolved_at
FROM dead_letter_messages
WHERE status NOT IN ('RESOLVED', 'REPROCESSED')
ORDER BY last_received_at;
```

O consumer não reexecuta operações financeiras automaticamente. O operador usa a CLI local, após investigar e corrigir a causa. Apenas mensagens `RETRY_EXHAUSTED` com envelope válido podem ser preparadas; mensagens inválidas e falhas permanentes não são elegíveis. `prepare` verifica se já existe transação financeira correspondente. Se não houver, `replay` grava uma solicitação durável na outbox administrativa e o publisher reenvia o corpo original com o mesmo messageId. O resultado é atualizado pelo consumidor depois do processamento; em falha, a mensagem permanece sujeita às regras de redelivery da fila. Não há endpoint HTTP administrativo, e a CLI deve ser executada por operador com acesso ao ambiente e ao banco.

```sh
bun run dead-letter -- list 100
bun run dead-letter -- prepare wager-transactions.fifo <message-id>
bun run dead-letter -- replay wager-transactions.fifo <message-id>
bun run dead-letter -- resolve wager-transactions.fifo <message-id>
bun run dead-letter -- retention
```

Os estados operacionais incluem `NEEDS_INVESTIGATION`, `READY_FOR_REPROCESS`, `REPLAY_QUEUED`, `REPROCESSING`, `REPROCESSED`, `REPROCESS_FAILED` e `RESOLVED`. Eles não representam estados da aposta e não alteram o saldo. A operação `replay` apenas agenda o envio durável; não executa a operação financeira dentro da CLI.

A retenção padrão é 90 dias, executada em lotes de 500 registros a cada 24 horas pelo worker. Configure `DEAD_LETTER_RETENTION_DAYS`, `DEAD_LETTER_RETENTION_BATCH_SIZE` e `DEAD_LETTER_RETENTION_INTERVAL_MS`. Só são removidos registros `RESOLVED` ou `REPROCESSED` cujo último marco operacional/financeiro seja anterior ao prazo. Registros associados a operações `PENDING`/`PENDING_REFERENCE` e replay ainda não publicado são protegidos. A rotina usa lock consultivo PostgreSQL para evitar limpezas simultâneas. Os registros financeiros, ledger e transações nunca são expurgados por essa rotina.

O estado financeiro `FAILED` foi retirado do contrato do domínio e do schema. Falhas transitórias revertem a transação SQL e são retentadas; rejeições de negócio permanecem `REJECTED`; mensagens esgotadas ou inválidas ficam no registro operacional da DLQ. Isso evita um estado de aposta ambíguo que não poderia ser gravado atomicamente com o resultado financeiro.

- Autenticação não está habilitada, conforme permitido no desafio. A porta `ProviderIdentityPort` documenta o ponto de extensão para validar credenciais de provedores; health e mensagens SQS são tratados como canais internos confiáveis.
- Esta entrega não inclui um consumidor da fila de eventos; publica os eventos no SQS `wager-events.fifo` para consumidores externos. O contrato downstream e a forma recomendada de persistir `eventId` junto ao efeito são descritos em `ARCHITECTURE.md`.
