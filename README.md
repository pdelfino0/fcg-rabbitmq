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

### Consumindo um evento

Implemente `IMessageProcessor` — a única responsabilidade dele é desserializar e despachar,
**sempre** retornando um `MessageProcessingResult` (nunca lançando exceção):

```csharp
public class UserRegisteredEventProcessor(IEventDispatcher dispatcher) : IMessageProcessor
{
    public async Task<MessageProcessingResult> ProcessAsync(
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken)
    {
        // desserializa, chama dispatcher.DispatchAsync(...), traduz falhas para
        // MessageProcessingResult.PoisonMessage / TransientFailure
    }
}
```

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

## Publicação

Automática via GitHub Actions: ao publicar uma **release** com tag `vX.Y.Z`, o pacote `X.Y.Z` é
enviado ao nuget.org via Trusted Publishing (OIDC) — mesmo esquema do `fcg-contracts`.

```bash
# Publicação manual (alternativa)
dotnet pack src/FiapCloudGames.RabbitMq/FiapCloudGames.RabbitMq.csproj -c Release -o out /p:Version=1.0.0
dotnet nuget push "out/*.nupkg" --api-key <API_KEY> --source https://api.nuget.org/v3/index.json
```
