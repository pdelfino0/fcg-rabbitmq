namespace FiapCloudGames.RabbitMq.Tests.Consumers;

using FiapCloudGames.RabbitMq.Configuration;
using FiapCloudGames.RabbitMq.Consumers;
using FiapCloudGames.RabbitMq.Processing;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

public class RabbitMqConsumerHostedServiceTests
{
    private readonly IConnectionFactory _connectionFactory = Substitute.For<IConnectionFactory>();
    private readonly IConnection _connection = Substitute.For<IConnection>();
    private readonly IChannel _channel = Substitute.For<IChannel>();
    private readonly IMessageProcessor _processor = Substitute.For<IMessageProcessor>();

    private readonly ILogger<RabbitMqConsumerHostedService<IMessageProcessor>>
        _logger = Substitute.For<ILogger<RabbitMqConsumerHostedService<IMessageProcessor>>>();

    private readonly RabbitMqConsumerDefinition _definition = new("test.exchange", "test.queue", "test.routing.key");
    private readonly RabbitMqSettings _settings = new() { MaxConnectionRetries = 2, ConnectionRetryDelayMs = 1 };

    private RabbitMqConsumerHostedService<IMessageProcessor> CreateService()
    {
        return new RabbitMqConsumerHostedService<IMessageProcessor>(
            _connectionFactory,
            Options.Create(_settings),
            _processor,
            _definition,
            _logger);
    }

    private IAsyncBasicConsumer CaptureConsumer()
    {
        IAsyncBasicConsumer? captured = null;
        _channel.BasicConsumeAsync(
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<bool>(),
                Arg.Any<IDictionary<string, object?>>(),
                Arg.Do<IAsyncBasicConsumer>(c => captured = c),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("consumer-tag"));

        return new LazyConsumer(() => captured!);
    }

    private sealed class LazyConsumer(Func<IAsyncBasicConsumer> resolve) : IAsyncBasicConsumer
    {
        public IAsyncBasicConsumer Resolved => resolve();

        public IChannel Channel => Resolved.Channel!;

        public Task HandleBasicCancelAsync(string consumerTag, CancellationToken cancellationToken = default) =>
            Resolved.HandleBasicCancelAsync(consumerTag, cancellationToken);

        public Task HandleBasicCancelOkAsync(string consumerTag, CancellationToken cancellationToken = default) =>
            Resolved.HandleBasicCancelOkAsync(consumerTag, cancellationToken);

        public Task HandleBasicConsumeOkAsync(string consumerTag, CancellationToken cancellationToken = default) =>
            Resolved.HandleBasicConsumeOkAsync(consumerTag, cancellationToken);

        public Task HandleBasicDeliverAsync(
            string consumerTag,
            ulong deliveryTag,
            bool redelivered,
            string exchange,
            string routingKey,
            RabbitMQ.Client.IReadOnlyBasicProperties properties,
            ReadOnlyMemory<byte> body,
            CancellationToken cancellationToken = default) =>
            Resolved.HandleBasicDeliverAsync(consumerTag, deliveryTag, redelivered, exchange, routingKey, properties, body, cancellationToken);

        public Task HandleChannelShutdownAsync(object channel, ShutdownEventArgs reason) =>
            Resolved.HandleChannelShutdownAsync(channel, reason);
    }

    public RabbitMqConsumerHostedServiceTests()
    {
        _connectionFactory.CreateConnectionAsync(Arg.Any<CancellationToken>()).Returns(_connection);
        _connection.CreateChannelAsync(Arg.Any<CreateChannelOptions?>(), Arg.Any<CancellationToken>()).Returns(_channel);
        _channel.BasicConsumeAsync(
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<bool>(),
                Arg.Any<IDictionary<string, object?>>(),
                Arg.Any<IAsyncBasicConsumer>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("consumer-tag"));
    }

    [Fact]
    public async Task StartAsync_DeclaresExchangeQueueAndBindingFromDefinition()
    {
        // Arrange
        var service = CreateService();

        // Act
        await service.StartAsync(CancellationToken.None);
        await service.Started.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        await _channel.Received(1).ExchangeDeclareAsync(
            _definition.Exchange,
            ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            arguments: Arg.Any<IDictionary<string, object?>>(),
            passive: false,
            noWait: false,
            cancellationToken: Arg.Any<CancellationToken>());

        await _channel.Received(1).QueueDeclareAsync(
            _definition.Queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: Arg.Any<IDictionary<string, object?>>(),
            passive: false,
            noWait: false,
            cancellationToken: Arg.Any<CancellationToken>());

        await _channel.Received(1).QueueBindAsync(
            _definition.Queue,
            _definition.Exchange,
            _definition.RoutingKey,
            Arg.Any<IDictionary<string, object?>>(),
            Arg.Any<bool>(),
            Arg.Any<CancellationToken>());

        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Consumer_WhenProcessorReturnsSuccess_AcksMessage()
    {
        // Arrange
        _processor.ProcessAsync(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<IReadOnlyDictionary<string, string?>>(), Arg.Any<CancellationToken>())
            .Returns(MessageProcessingResult.Success);

        IAsyncBasicConsumer consumer = CaptureConsumer();
        var service = CreateService();

        // Act
        await service.StartAsync(CancellationToken.None);
        await service.Started.WaitAsync(TimeSpan.FromSeconds(5));
        await consumer.HandleBasicDeliverAsync(
            "consumer-tag", 42UL, false, _definition.Exchange, _definition.RoutingKey,
            Substitute.For<RabbitMQ.Client.IReadOnlyBasicProperties>(), new byte[] { 1, 2, 3 }, CancellationToken.None);

        // Assert
        await _channel.Received(1).BasicAckAsync(42UL, false, Arg.Any<CancellationToken>());
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Consumer_WhenProcessorReturnsPoisonMessage_NacksWithoutRequeue()
    {
        // Arrange
        _processor.ProcessAsync(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<IReadOnlyDictionary<string, string?>>(), Arg.Any<CancellationToken>())
            .Returns(MessageProcessingResult.PoisonMessage);

        IAsyncBasicConsumer consumer = CaptureConsumer();
        var service = CreateService();

        // Act
        await service.StartAsync(CancellationToken.None);
        await service.Started.WaitAsync(TimeSpan.FromSeconds(5));
        await consumer.HandleBasicDeliverAsync(
            "consumer-tag", 7UL, false, _definition.Exchange, _definition.RoutingKey,
            Substitute.For<RabbitMQ.Client.IReadOnlyBasicProperties>(), new byte[] { 1 }, CancellationToken.None);

        // Assert
        await _channel.Received(1).BasicNackAsync(7UL, false, false, Arg.Any<CancellationToken>());
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Consumer_WhenProcessorReturnsTransientFailure_NacksWithRequeue()
    {
        // Arrange
        _processor.ProcessAsync(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<IReadOnlyDictionary<string, string?>>(), Arg.Any<CancellationToken>())
            .Returns(MessageProcessingResult.TransientFailure);

        IAsyncBasicConsumer consumer = CaptureConsumer();
        var service = CreateService();

        // Act
        await service.StartAsync(CancellationToken.None);
        await service.Started.WaitAsync(TimeSpan.FromSeconds(5));
        await consumer.HandleBasicDeliverAsync(
            "consumer-tag", 9UL, false, _definition.Exchange, _definition.RoutingKey,
            Substitute.For<RabbitMQ.Client.IReadOnlyBasicProperties>(), new byte[] { 1 }, CancellationToken.None);

        // Assert
        await _channel.Received(1).BasicNackAsync(9UL, false, true, Arg.Any<CancellationToken>());
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Consumer_WhenMessageHasHeaders_DeliversThemNormalizedAsStrings()
    {
        // Arrange
        IReadOnlyDictionary<string, string?>? received = null;
        _processor.ProcessAsync(
                Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Do<IReadOnlyDictionary<string, string?>>(h => received = h),
                Arg.Any<CancellationToken>())
            .Returns(MessageProcessingResult.Success);

        // O cliente RabbitMQ devolve strings do AMQP field table como byte[], não como string.
        var properties = Substitute.For<RabbitMQ.Client.IReadOnlyBasicProperties>();
        properties.Headers.Returns(new Dictionary<string, object?>
        {
            ["traceparent"] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"u8.ToArray(),
            ["x-attempt"] = 3
        });

        IAsyncBasicConsumer consumer = CaptureConsumer();
        var service = CreateService();

        // Act
        await service.StartAsync(CancellationToken.None);
        await service.Started.WaitAsync(TimeSpan.FromSeconds(5));
        await consumer.HandleBasicDeliverAsync(
            "consumer-tag", 1UL, false, _definition.Exchange, _definition.RoutingKey,
            properties, new byte[] { 1 }, CancellationToken.None);

        // Assert
        received.Should().NotBeNull();
        received!["traceparent"].Should().Be("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        received["x-attempt"].Should().Be("3");
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Consumer_WhenMessageHasNoHeaders_DeliversEmptyDictionaryNotNull()
    {
        // Arrange
        IReadOnlyDictionary<string, string?>? received = null;
        _processor.ProcessAsync(
                Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Do<IReadOnlyDictionary<string, string?>>(h => received = h),
                Arg.Any<CancellationToken>())
            .Returns(MessageProcessingResult.Success);

        var properties = Substitute.For<RabbitMQ.Client.IReadOnlyBasicProperties>();
        properties.Headers.Returns((IDictionary<string, object?>?)null);

        IAsyncBasicConsumer consumer = CaptureConsumer();
        var service = CreateService();

        // Act
        await service.StartAsync(CancellationToken.None);
        await service.Started.WaitAsync(TimeSpan.FromSeconds(5));
        await consumer.HandleBasicDeliverAsync(
            "consumer-tag", 2UL, false, _definition.Exchange, _definition.RoutingKey,
            properties, new byte[] { 1 }, CancellationToken.None);

        // Assert
        received.Should().NotBeNull();
        received.Should().BeEmpty();
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StartAsync_WhenConnectionFailsBelowMaxRetries_RetriesUntilConnected()
    {
        // Arrange
        var attempts = 0;
        _connectionFactory.CreateConnectionAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            attempts++;
            if (attempts <= 2)
            {
                throw new InvalidOperationException("conexão recusada");
            }

            return Task.FromResult(_connection);
        });

        var service = CreateService();

        // Act
        await service.StartAsync(CancellationToken.None);
        await service.Started.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        attempts.Should().Be(3);
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StopAsync_CancelsGracefullyWithoutThrowing()
    {
        // Arrange
        var service = CreateService();
        await service.StartAsync(CancellationToken.None);
        await service.Started.WaitAsync(TimeSpan.FromSeconds(5));

        // Act
        Func<Task> act = () => service.StopAsync(CancellationToken.None);

        // Assert
        await act.Should().NotThrowAsync();
    }
}
