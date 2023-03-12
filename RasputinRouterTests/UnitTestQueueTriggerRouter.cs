using System;
using Xunit;
using Moq;
using Microsoft.Extensions.Logging;
using Rasputin.Router;

namespace RasputinRouterTests;

public class UnitTestQueueTriggerRouter
{
    [Fact]
    public async void UnitTestQueueTriggerRouterTestInvalidMessage()
    {
        // Arrange
        var mock = new Mock<ILogger>();
        var logger = mock.Object;

        // Act
        var sut = new QueueTriggerRouter();
        // Call RunAsync with null message and expect ArgumentNullException
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await sut.RunAsync(null, logger));

    }
}