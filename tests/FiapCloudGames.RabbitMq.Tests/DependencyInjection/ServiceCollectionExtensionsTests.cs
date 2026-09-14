namespace FiapCloudGames.RabbitMq.Tests.DependencyInjection;

using FiapCloudGames.RabbitMq.Configuration;
using FiapCloudGames.RabbitMq.Consumers;
using FiapCloudGames.RabbitMq.DependencyInjection;
using FiapCloudGames.RabbitMq.Processing;
using FiapCloudGames.RabbitMq.Publishers;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

public class ServiceCollectionExtensionsTests
{
    private sealed class FakeProcessorA : IMessageProcessor
    {
        public Task<MessageProcessingResult> ProcessAsync(
            ReadOnlyMemory<byte> body,
            IReadOnlyDictionary<string, string?> headers,
            CancellationToken cancellationToken) =>
            Task.FromResult(MessageProcessingResult.Success);
    }

    private sealed class FakeProcessorB : IMessageProcessor
    {
        public Task<MessageProcessingResult> ProcessAsync(
            ReadOnlyMemory<byte> body,
            IReadOnlyDictionary<string, string?> headers,
            CancellationToken cancellationToken) =>
            Task.FromResult(MessageProcessingResult.Success);
    }

    private static ServiceCollection CreateServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        return services;
    }

    private static IConfiguration BuildConfiguration()
    {
        return new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RabbitMq:Host"] = "broker.local",
                ["RabbitMq:Port"] = "5673",
                ["RabbitMq:Username"] = "fcg",
                ["RabbitMq:Password"] = "fcg123",
                ["RabbitMq:VirtualHost"] = "/fcg",
                ["RabbitMq:MaxConnectionRetries"] = "5",
                ["RabbitMq:ConnectionRetryDelayMs"] = "2000"
            })
            .Build();
    }

    [Fact]
    public void AddRabbitMq_RegistersSettingsPublisherAndConnectionFactory()
    {
        // Arrange
        var services = CreateServices();

        // Act
        services.AddRabbitMq(BuildConfiguration());
        ServiceProvider provider = services.BuildServiceProvider();

        // Assert
        RabbitMqSettings settings = provider.GetRequiredService<IOptions<RabbitMqSettings>>().Value;
        settings.Host.Should().Be("broker.local");
        settings.Port.Should().Be(5673);
        settings.Username.Should().Be("fcg");
        settings.MaxConnectionRetries.Should().Be(5);

        provider.GetRequiredService<IConnectionFactory>().Should().NotBeNull();
        provider.GetRequiredService<IRabbitMqPublisher>().Should().NotBeNull();
    }

    [Fact]
    public void AddRabbitMq_ConnectionFactory_UsesBoundSettings()
    {
        // Arrange
        var services = CreateServices();
        services.AddRabbitMq(BuildConfiguration());
        ServiceProvider provider = services.BuildServiceProvider();

        // Act
        var factory = (ConnectionFactory)provider.GetRequiredService<IConnectionFactory>();

        // Assert
        factory.HostName.Should().Be("broker.local");
        factory.Port.Should().Be(5673);
        factory.UserName.Should().Be("fcg");
        factory.Password.Should().Be("fcg123");
        factory.VirtualHost.Should().Be("/fcg");
    }

    [Fact]
    public void AddRabbitMqConsumer_RegistersProcessorAndHostedService()
    {
        // Arrange
        var services = CreateServices();
        services.AddRabbitMq(BuildConfiguration());

        // Act
        services.AddRabbitMqConsumer<FakeProcessorA>(new RabbitMqConsumerDefinition("ex", "queue-a", "routing.a"));
        ServiceProvider provider = services.BuildServiceProvider();

        // Assert
        provider.GetRequiredService<FakeProcessorA>().Should().NotBeNull();
        provider.GetRequiredService<RabbitMqConsumerHostedService<FakeProcessorA>>().Should().NotBeNull();
        provider.GetServices<IHostedService>().Should().ContainSingle(s => s is RabbitMqConsumerHostedService<FakeProcessorA>);
    }

    [Fact]
    public void AddRabbitMqConsumer_WithMultipleProcessorTypes_DoesNotCollide()
    {
        // Arrange
        var services = CreateServices();
        services.AddRabbitMq(BuildConfiguration());
        services.AddRabbitMqConsumer<FakeProcessorA>(new RabbitMqConsumerDefinition("ex", "queue-a", "routing.a"));
        services.AddRabbitMqConsumer<FakeProcessorB>(new RabbitMqConsumerDefinition("ex", "queue-b", "routing.b"));

        // Act
        ServiceProvider provider = services.BuildServiceProvider();
        var hostedServices = provider.GetServices<IHostedService>().ToList();

        // Assert
        hostedServices.Should().ContainSingle(s => s is RabbitMqConsumerHostedService<FakeProcessorA>);
        hostedServices.Should().ContainSingle(s => s is RabbitMqConsumerHostedService<FakeProcessorB>);
    }
}
