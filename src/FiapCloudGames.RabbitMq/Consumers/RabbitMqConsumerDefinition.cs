namespace FiapCloudGames.RabbitMq.Consumers;

/// <summary>
/// Descreve a topologia RabbitMQ (exchange, fila e routing key) que um consumidor deve declarar
/// e vincular no boot, removendo strings hardcoded do <see cref="RabbitMqConsumerHostedService{TProcessor}"/>.
/// </summary>
public sealed record RabbitMqConsumerDefinition(string Exchange, string Queue, string RoutingKey);
