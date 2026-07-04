namespace FiapCloudGames.RabbitMq.DependencyInjection;

using Configuration;
using Consumers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Processing;
using Publishers;
using RabbitMQ.Client;

/// <summary>
/// Métodos de extensão para registrar a infraestrutura RabbitMQ no contêiner de injeção de dependência.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registra <see cref="RabbitMqSettings"/> (vinculado à seção <see cref="RabbitMqSettings.SectionName"/>),
    /// um <see cref="IConnectionFactory"/> e o <see cref="IRabbitMqPublisher"/>.
    /// </summary>
    public static IServiceCollection AddRabbitMq(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<RabbitMqSettings>(configuration.GetSection(RabbitMqSettings.SectionName));

        services.AddSingleton<IConnectionFactory>(sp =>
        {
            RabbitMqSettings settings = sp.GetRequiredService<IOptions<RabbitMqSettings>>().Value;
            return new ConnectionFactory
            {
                HostName = settings.Host,
                Port = settings.Port,
                UserName = settings.Username,
                Password = settings.Password,
                VirtualHost = settings.VirtualHost
            };
        });

        services.AddSingleton<IRabbitMqPublisher, RabbitMqPublisher>();

        return services;
    }

    /// <summary>
    /// Registra <typeparamref name="TProcessor"/> e sobe um <see cref="RabbitMqConsumerHostedService{TProcessor}"/>
    /// que declara e consome a fila descrita por <paramref name="definition"/>.
    /// </summary>
    public static IServiceCollection AddRabbitMqConsumer<TProcessor>(
        this IServiceCollection services,
        RabbitMqConsumerDefinition definition)
        where TProcessor : class, IMessageProcessor
    {
        services.AddSingleton<TProcessor>();

        services.AddSingleton(sp => new RabbitMqConsumerHostedService<TProcessor>(
            sp.GetRequiredService<IConnectionFactory>(),
            sp.GetRequiredService<IOptions<RabbitMqSettings>>(),
            sp.GetRequiredService<TProcessor>(),
            definition,
            sp.GetRequiredService<ILogger<RabbitMqConsumerHostedService<TProcessor>>>()));

        services.AddHostedService(sp => sp.GetRequiredService<RabbitMqConsumerHostedService<TProcessor>>());

        return services;
    }
}
