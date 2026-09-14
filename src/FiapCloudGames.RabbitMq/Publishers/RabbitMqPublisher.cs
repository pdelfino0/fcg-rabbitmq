namespace FiapCloudGames.RabbitMq.Publishers;

using System.Text.Json;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

/// <summary>
/// Implementação de <see cref="IRabbitMqPublisher"/>. Mantém uma conexão de longa duração
/// (canais não são seguros para uso concorrente, então um canal novo é aberto por publicação) e
/// é segura para chamadas concorrentes de <c>PublishAsync</c>.
/// </summary>
public sealed partial class RabbitMqPublisher(
    IConnectionFactory connectionFactory,
    ILogger<RabbitMqPublisher> logger) : IRabbitMqPublisher, IAsyncDisposable
{
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private IConnection? _connection;

    public Task PublishAsync<T>(string exchange, string routingKey, T message, CancellationToken cancellationToken)
    {
        return PublishAsync(exchange, routingKey, message, headers: null, cancellationToken);
    }

    public async Task PublishAsync<T>(
        string exchange,
        string routingKey,
        T message,
        IDictionary<string, object?>? headers,
        CancellationToken cancellationToken)
    {
        try
        {
            IConnection connection = await GetConnectionAsync(cancellationToken);
            await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

            await DeclareExchangeAsync(channel, exchange, cancellationToken);

            byte[] body = JsonSerializer.SerializeToUtf8Bytes(message);
            var properties = new BasicProperties
            {
                ContentType = "application/json",
                DeliveryMode = DeliveryModes.Persistent
            };

            // Mensagem sem header algum não deve ganhar uma tabela de headers vazia nas properties.
            if (headers is { Count: > 0 })
            {
                properties.Headers = new Dictionary<string, object?>(headers);
            }

            await channel.BasicPublishAsync(exchange, routingKey, mandatory: false, properties, body, cancellationToken);

            LogMessagePublished(typeof(T).Name, exchange, routingKey);
        }
        catch (Exception ex)
        {
            LogPublishFailed(ex, typeof(T).Name, exchange, routingKey);
            throw;
        }
    }

    private static Task DeclareExchangeAsync(IChannel channel, string exchange, CancellationToken cancellationToken)
    {
        return channel.ExchangeDeclareAsync(
            exchange,
            ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);
    }

    private async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection is { IsOpen: true })
        {
            return _connection;
        }

        await _connectionLock.WaitAsync(cancellationToken);
        try
        {
            if (_connection is { IsOpen: true })
            {
                return _connection;
            }

            if (_connection is not null)
            {
                await _connection.DisposeAsync();
            }

            _connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }

        _connectionLock.Dispose();
    }

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Information,
        Message = "Evento {EventType} publicado em {Exchange} ({RoutingKey})")]
    private partial void LogMessagePublished(string eventType, string exchange, string routingKey);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Error,
        Message = "Falha ao publicar evento {EventType} em {Exchange} ({RoutingKey})")]
    private partial void LogPublishFailed(Exception exception, string eventType, string exchange, string routingKey);
}
