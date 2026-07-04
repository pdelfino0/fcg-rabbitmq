namespace FiapCloudGames.RabbitMq.Tests.Publishers;

using System.Text.Json;
using FiapCloudGames.RabbitMq.Publishers;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using NSubstitute;
using RabbitMQ.Client;

public class RabbitMqPublisherTests
{
    private readonly IConnectionFactory _connectionFactory = Substitute.For<IConnectionFactory>();
    private readonly IConnection _connection = Substitute.For<IConnection>();
    private readonly IChannel _channel = Substitute.For<IChannel>();
    private readonly ILogger<RabbitMqPublisher> _logger = Substitute.For<ILogger<RabbitMqPublisher>>();
    private readonly RabbitMqPublisher _publisher;

    private record TestEvent(Guid Id, string Name);

    public RabbitMqPublisherTests()
    {
        _connection.IsOpen.Returns(true);
        _connectionFactory.CreateConnectionAsync(Arg.Any<CancellationToken>()).Returns(_connection);
        _connection.CreateChannelAsync(Arg.Any<CreateChannelOptions?>(), Arg.Any<CancellationToken>()).Returns(_channel);

        _publisher = new RabbitMqPublisher(_connectionFactory, _logger);
    }

    [Fact]
    public async Task PublishAsync_DeclaresExchangeAsTopicDurable()
    {
        // Arrange
        var message = new TestEvent(Guid.NewGuid(), "Jane");

        // Act
        await _publisher.PublishAsync("orders.exchange", "order.placed", message, CancellationToken.None);

        // Assert
        await _channel.Received(1).ExchangeDeclareAsync(
            "orders.exchange",
            ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            arguments: Arg.Any<IDictionary<string, object?>>(),
            passive: false,
            noWait: false,
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_SerializesMessageAndPublishesAsPersistent()
    {
        // Arrange
        var message = new TestEvent(Guid.NewGuid(), "Jane");
        byte[] expectedBody = JsonSerializer.SerializeToUtf8Bytes(message);

        // Act
        await _publisher.PublishAsync("orders.exchange", "order.placed", message, CancellationToken.None);

        // Assert
        await _channel.Received(1).BasicPublishAsync(
            "orders.exchange",
            "order.placed",
            false,
            Arg.Is<BasicProperties>(p => p.DeliveryMode == DeliveryModes.Persistent && p.ContentType == "application/json"),
            Arg.Is<ReadOnlyMemory<byte>>(body => body.ToArray().SequenceEqual(expectedBody)),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_PropagatesCancellationToken()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var message = new TestEvent(Guid.NewGuid(), "Jane");

        // Act
        await _publisher.PublishAsync("orders.exchange", "order.placed", message, cts.Token);

        // Assert
        await _channel.Received(1).BasicPublishAsync(
            Arg.Any<string>(),
            Arg.Any<string>(),
            Arg.Any<bool>(),
            Arg.Any<BasicProperties>(),
            Arg.Any<ReadOnlyMemory<byte>>(),
            cts.Token);
    }

    [Fact]
    public async Task PublishAsync_WhenChannelDeclarationFails_PropagatesException()
    {
        // Arrange
        var expectedException = new InvalidOperationException("broker indisponível");
        _channel.ExchangeDeclareAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<bool>(),
                Arg.Any<IDictionary<string, object?>>(),
                Arg.Any<bool>(),
                Arg.Any<bool>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromException(expectedException));

        var message = new TestEvent(Guid.NewGuid(), "Jane");

        // Act
        Func<Task> act = () => _publisher.PublishAsync("orders.exchange", "order.placed", message, CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("broker indisponível");
    }

    [Fact]
    public async Task PublishAsync_WithOpenConnection_ReusesConnectionAcrossCalls()
    {
        // Arrange
        var message = new TestEvent(Guid.NewGuid(), "Jane");

        // Act
        await _publisher.PublishAsync("orders.exchange", "order.placed", message, CancellationToken.None);
        await _publisher.PublishAsync("orders.exchange", "order.placed", message, CancellationToken.None);

        // Assert
        await _connectionFactory.Received(1).CreateConnectionAsync(Arg.Any<CancellationToken>());
    }
}
