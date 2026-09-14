namespace FiapCloudGames.RabbitMq.Publishers;

/// <summary>
/// Publica mensagens em uma exchange do RabbitMQ. Seguro para uso concorrente.
/// </summary>
public interface IRabbitMqPublisher
{
    /// <summary>
    /// Serializa <paramref name="message"/> com <see cref="System.Text.Json.JsonSerializer"/> e
    /// publica de forma persistente em <paramref name="exchange"/>/<paramref name="routingKey"/>,
    /// declarando a exchange automaticamente se necessário. Equivale a chamar a sobrecarga com
    /// <c>headers: null</c>.
    /// </summary>
    Task PublishAsync<T>(string exchange, string routingKey, T message, CancellationToken cancellationToken);

    /// <summary>
    /// Igual à sobrecarga sem <paramref name="headers"/>, mas anexa headers de aplicação à mensagem
    /// (por exemplo, contexto de trace distribuído como <c>traceparent</c>).
    /// </summary>
    /// <param name="headers">
    /// Headers a anexar. Quando <see langword="null"/> ou vazio, a mensagem é publicada sem tabela de
    /// headers. Valores <see cref="string"/> trafegam como <em>long string</em> no AMQP e são lidos de
    /// volta como <c>byte[]</c> pelo cliente — o consumidor deste pacote já normaliza isso para
    /// <see cref="string"/> antes de entregar ao <c>IMessageProcessor</c>.
    /// </param>
    Task PublishAsync<T>(
        string exchange,
        string routingKey,
        T message,
        IDictionary<string, object?>? headers,
        CancellationToken cancellationToken);
}
