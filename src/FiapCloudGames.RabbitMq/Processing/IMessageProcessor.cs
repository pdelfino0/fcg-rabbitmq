namespace FiapCloudGames.RabbitMq.Processing;

using Consumers;

/// <summary>
/// Representa um objeto capaz de processar o corpo bruto de uma mensagem consumida do RabbitMQ.
/// </summary>
/// <remarks>
/// A implementação nunca deve deixar uma exceção escapar de <see cref="ProcessAsync"/> — falhas
/// devem ser capturadas e traduzidas para <see cref="MessageProcessingResult.PoisonMessage"/>
/// (mensagem malformada/não recuperável) ou <see cref="MessageProcessingResult.TransientFailure"/>
/// (falha passageira que justifica reentrega). Tipicamente a implementação desserializa o corpo
/// e despacha para o handler de aplicação correspondente.
/// </remarks>
public interface IMessageProcessor
{
    /// <summary>
    /// Processa o corpo bruto de uma mensagem, junto com os headers que vieram com ela.
    /// </summary>
    /// <param name="body">Corpo bruto da mensagem.</param>
    /// <param name="headers">
    /// Headers da mensagem, já normalizados para <see cref="string"/> (o AMQP entrega <c>byte[]</c>).
    /// Nunca é <see langword="null"/>: mensagem sem headers chega como dicionário vazio. As chaves
    /// são comparadas de forma case-insensitive.
    /// </param>
    /// <param name="cancellationToken">Token de cancelamento.</param>
    Task<MessageProcessingResult> ProcessAsync(
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, string?> headers,
        CancellationToken cancellationToken);
}
