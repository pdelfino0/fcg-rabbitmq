namespace FiapCloudGames.RabbitMq.Publishers;

/// <summary>
/// Publica mensagens em uma exchange do RabbitMQ. Seguro para uso concorrente.
/// </summary>
public interface IRabbitMqPublisher
{
    /// <summary>
    /// Serializa <paramref name="message"/> com <see cref="System.Text.Json.JsonSerializer"/> e
    /// publica de forma persistente em <paramref name="exchange"/>/<paramref name="routingKey"/>,
    /// declarando a exchange automaticamente se necessário.
    /// </summary>
    Task PublishAsync<T>(string exchange, string routingKey, T message, CancellationToken cancellationToken);
}
