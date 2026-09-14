namespace FiapCloudGames.RabbitMq.Processing;

using System.Text;

/// <summary>
/// Converte a tabela de headers AMQP entregue pelo cliente RabbitMQ em um dicionário de
/// <see cref="string"/> pronto para consumo pelo <see cref="IMessageProcessor"/>.
/// </summary>
/// <remarks>
/// O AMQP transporta headers como <em>field table</em>: o cliente .NET serializa <see cref="string"/>
/// como <em>long string</em> e devolve <c>byte[]</c> na leitura. Sem essa normalização, todo consumidor
/// precisaria conhecer esse detalhe de transporte.
/// </remarks>
public static class MessageHeaders
{
    /// <summary>
    /// Dicionário vazio entregue quando a mensagem não traz headers.
    /// </summary>
    public static IReadOnlyDictionary<string, string?> Empty { get; } =
        new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Normaliza os headers brutos da mensagem para <see cref="string"/>.
    /// Retorna <see cref="Empty"/> quando <paramref name="rawHeaders"/> é <see langword="null"/> ou vazio.
    /// </summary>
    public static IReadOnlyDictionary<string, string?> Normalize(IDictionary<string, object?>? rawHeaders)
    {
        if (rawHeaders is not { Count: > 0 })
        {
            return Empty;
        }

        var normalized = new Dictionary<string, string?>(rawHeaders.Count, StringComparer.OrdinalIgnoreCase);
        foreach (KeyValuePair<string, object?> header in rawHeaders)
        {
            normalized[header.Key] = ToHeaderValue(header.Value);
        }

        return normalized;
    }

    /// <summary>
    /// Converte um valor bruto de header: <c>byte[]</c> é decodificado como UTF-8; os demais tipos
    /// usam <see cref="object.ToString"/>; <see langword="null"/> permanece <see langword="null"/>.
    /// </summary>
    public static string? ToHeaderValue(object? raw) => raw switch
    {
        null => null,
        string text => text,
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        ReadOnlyMemory<byte> memory => Encoding.UTF8.GetString(memory.Span),
        _ => raw.ToString()
    };
}
