namespace FiapCloudGames.RabbitMq.Consumers;

/// <summary>
/// Resultado do processamento de uma mensagem consumida do RabbitMQ.
/// </summary>
public enum MessageProcessingResult
{
    /// <summary>
    /// Mensagem processada com sucesso; deve ser confirmada (ack).
    /// </summary>
    Success,

    /// <summary>
    /// Mensagem corrompida, inválida ou já processada; não deve ser reenfileirada.
    /// </summary>
    PoisonMessage,

    /// <summary>
    /// Falha transitória ao processar a mensagem; deve ser reenfileirada.
    /// </summary>
    TransientFailure
}
