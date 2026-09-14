namespace FiapCloudGames.RabbitMq.Tests.Processing;

using System.Text;
using FiapCloudGames.RabbitMq.Processing;
using FluentAssertions;

public class MessageHeadersTests
{
    [Fact]
    public void Normalize_WhenValueIsByteArray_DecodesAsUtf8String()
    {
        // Arrange
        var raw = new Dictionary<string, object?>
        {
            ["traceparent"] = Encoding.UTF8.GetBytes("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        };

        // Act
        IReadOnlyDictionary<string, string?> normalized = MessageHeaders.Normalize(raw);

        // Assert
        normalized["traceparent"].Should().Be("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    }

    [Fact]
    public void Normalize_WhenValueIsNotAString_UsesToString()
    {
        // Arrange
        var raw = new Dictionary<string, object?> { ["x-attempt"] = 7, ["x-flag"] = true };

        // Act
        IReadOnlyDictionary<string, string?> normalized = MessageHeaders.Normalize(raw);

        // Assert
        normalized["x-attempt"].Should().Be("7");
        normalized["x-flag"].Should().Be("True");
    }

    [Fact]
    public void Normalize_WhenValueIsNull_KeepsNullWithoutLosingTheKey()
    {
        // Arrange
        var raw = new Dictionary<string, object?> { ["empty"] = null };

        // Act
        IReadOnlyDictionary<string, string?> normalized = MessageHeaders.Normalize(raw);

        // Assert
        normalized.Should().ContainKey("empty");
        normalized["empty"].Should().BeNull();
    }

    [Fact]
    public void Normalize_IsCaseInsensitiveOnKeys()
    {
        // Arrange
        var raw = new Dictionary<string, object?> { ["TraceParent"] = "00-trace-span-01"u8.ToArray() };

        // Act
        IReadOnlyDictionary<string, string?> normalized = MessageHeaders.Normalize(raw);

        // Assert
        normalized["traceparent"].Should().Be("00-trace-span-01");
    }

    [Fact]
    public void Normalize_WhenHeadersAreNullOrEmpty_ReturnsEmptyDictionaryNotNull()
    {
        // Act
        IReadOnlyDictionary<string, string?> fromNull = MessageHeaders.Normalize(null);
        IReadOnlyDictionary<string, string?> fromEmpty = MessageHeaders.Normalize(new Dictionary<string, object?>());

        // Assert
        fromNull.Should().NotBeNull().And.BeEmpty();
        fromEmpty.Should().NotBeNull().And.BeEmpty();
    }
}
