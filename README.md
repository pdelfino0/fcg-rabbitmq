# FiapCloudGames.RabbitMq

Infraestrutura de mensageria RabbitMQ opinativa e reutilizável entre os microsserviços do FIAP
Cloud Games. Não é um framework de mensageria genérico — é a integração RabbitMQ padrão do
ecossistema FCG, pensada para reduzir drasticamente a quantidade de código de infraestrutura que
cada serviço precisa escrever, mantendo tudo explícito e testável.

O pacote cuida só de infraestrutura (conexão, retry, declaração de exchange/fila/binding,
publish, consumo, ack/nack). Não conhece tipos de evento, handlers ou regras de negócio — isso
continua em cada microsserviço.

## Instalação

```bash
dotnet add package FiapCloudGames.RabbitMq
```

## Uso

### Configuração

`appsettings.json`:

```json
{
  "RabbitMq": {
    "Host": "localhost",
    "Port": 5672,
    "Username": "guest",
    "Password": "guest",
    "VirtualHost": "/",
    "MaxConnectionRetries": 3,
    "ConnectionRetryDelayMs": 1000
  }
}
```

`Program.cs`:

```csharp
builder.Services.AddRabbitMq(builder.Configuration);
```

Isso registra `RabbitMqSettings`, um `IConnectionFactory` e o `IRabbitMqPublisher`.

### Publicando um evento

```csharp
await publisher.PublishAsync(
    exchange: "users.exchange",
    routingKey: "user.registered",
    message: new UserRegisteredEvent(userId, name, email),
    cancellationToken);
```

A exchange (`topic`, durável) é declarada automaticamente antes da publicação. A mensagem é
serializada com `System.Text.Json` e publicada como persistente.

#### Publicando com headers

Há uma sobrecarga que anexa headers de aplicação à mensagem — é o que permite propagar contexto
de trace distribuído (`traceparent`, `tracestate`, …) através do broker:

```csharp
await publisher.PublishAsync(
    exchange: "catalog.exchange",
    routingKey: "order.placed",
    message: new OrderPlacedEvent(orderId, userId, amount),
    headers: new Dictionary<string, object?>
    {
        ["traceparent"] = Activity.Current?.Id
    },
    cancellationToken);
```

O pacote **não** conhece semântica de tracing: ele só transporta os headers. Qual header injetar
(e como) é decisão de quem consome o pacote. Headers `null` ou vazios não geram tabela de headers
na mensagem — a publicação sem headers continua idêntica à da versão 1.0.0.

### Consumindo um evento

Implemente `IMessageProcessor` — a única responsabilidade dele é desserializar e despachar,
**sempre** retornando um `MessageProcessingResult` (nunca lançando exceção):

```csharp
public class UserRegisteredEventProcessor(IEventDispatcher dispatcher) : IMessageProcessor
{
    public async Task<MessageProcessingResult> ProcessAsync(
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, string?> headers,
        CancellationToken cancellationToken)
    {
        // headers["traceparent"] já vem como string legível
        // desserializa, chama dispatcher.DispatchAsync(...), traduz falhas para
        // MessageProcessingResult.PoisonMessage / TransientFailure
    }
}
```

Sobre `headers`:

- nunca é `null` — mensagem sem headers chega como dicionário **vazio**;
- os valores já vêm normalizados para `string`. O AMQP transporta headers como *field table* e o
  cliente .NET devolve `string` como `byte[]` na leitura; o pacote faz essa conversão (UTF-8) para
  que nenhum consumidor precise conhecer o detalhe do transporte;
- as chaves são comparadas de forma *case-insensitive*.

Registre o consumidor:

```csharp
builder.Services.AddRabbitMqConsumer<UserRegisteredEventProcessor>(
    new RabbitMqConsumerDefinition(
        Exchange: "users.exchange",
        Queue: "notifications.user-registered",
        RoutingKey: "user.registered"));
```

Isso registra o `TProcessor`, declara a exchange/fila/binding no boot (de forma idempotente) e
sobe um `BackgroundService` que consome, chama o processor e faz `ack`/`nack` de acordo com o
`MessageProcessingResult` retornado:

- `Success` → `ack`
- `PoisonMessage` (mensagem corrompida ou já processada) → `nack` sem reenfileirar
- `TransientFailure` (falha passageira) → `nack` reenfileirando

Nenhum consumidor precisa interagir diretamente com `ConnectionFactory`, `IChannel`,
`ExchangeDeclare`, `QueueDeclare`, `BasicConsume` ou `BasicPublish`.

## Versionamento

Segue [SemVer](https://semver.org).

### Migrando de 1.0.0 para 1.1.0 (mudança incompatível)

A versão 1.1.0 altera a assinatura de `IMessageProcessor.ProcessAsync`, que passa a receber os
headers da mensagem:

```diff
-Task<MessageProcessingResult> ProcessAsync(ReadOnlyMemory<byte> body, CancellationToken cancellationToken);
+Task<MessageProcessingResult> ProcessAsync(
+    ReadOnlyMemory<byte> body,
+    IReadOnlyDictionary<string, string?> headers,
+    CancellationToken cancellationToken);
```

Toda implementação de `IMessageProcessor` precisa adicionar o parâmetro `headers` (basta ignorá-lo
se o processor não usar headers). A quebra é de compilação, não de runtime, e foi preferida a um
*default interface method* para manter uma única porta de entrada no contrato — os únicos
consumidores do pacote estão sob nosso controle e são migrados no mesmo ciclo.

`IRabbitMqPublisher` **não** quebra: a assinatura antiga de `PublishAsync` continua existindo e
delega para a nova com `headers: null`.

## Publicação

Automática via GitHub Actions: ao publicar uma **release** com tag `vX.Y.Z`, o pacote `X.Y.Z` é
enviado ao nuget.org via Trusted Publishing (OIDC) — mesmo esquema do `fcg-contracts`.

```bash
# Publicação manual (alternativa)
dotnet pack src/FiapCloudGames.RabbitMq/FiapCloudGames.RabbitMq.csproj -c Release -o out /p:Version=1.0.0
dotnet nuget push "out/*.nupkg" --api-key <API_KEY> --source https://api.nuget.org/v3/index.json
```
