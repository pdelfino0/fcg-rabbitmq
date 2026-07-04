namespace FiapCloudGames.RabbitMq.Configuration;

/// <summary>
/// Configurações de conexão com o RabbitMQ, vinculáveis a partir de <see cref="Microsoft.Extensions.Configuration.IConfiguration"/>
/// através da seção <see cref="SectionName"/>.
/// </summary>
public class RabbitMqSettings
{
    public const string SectionName = "RabbitMq";

    /// <summary>
    /// Nome do host ou endereço IP do RabbitMQ.
    /// </summary>
    public string Host { get; set; } = "localhost";

    /// <summary>
    /// Número da porta do RabbitMQ.
    /// </summary>
    public int Port { get; set; } = 5672;

    /// <summary>
    /// Nome de usuário do RabbitMQ para autenticação.
    /// </summary>
    public string Username { get; set; } = "guest";

    /// <summary>
    /// Senha do RabbitMQ para autenticação.
    /// </summary>
    public string Password { get; set; } = "guest";

    /// <summary>
    /// Host virtual do RabbitMQ.
    /// </summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// Número máximo de tentativas de conexão antes de aguardar e tentar novamente.
    /// </summary>
    public int MaxConnectionRetries { get; set; } = 3;

    /// <summary>
    /// Atraso em milissegundos entre tentativas de conexão.
    /// </summary>
    public int ConnectionRetryDelayMs { get; set; } = 1000;
}
