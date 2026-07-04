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
    /// Processa o corpo bruto de uma mensagem.
    /// </summary>
    Task<MessageProcessingResult> ProcessAsync(ReadOnlyMemory<byte> body, CancellationToken cancellationToken);
}
