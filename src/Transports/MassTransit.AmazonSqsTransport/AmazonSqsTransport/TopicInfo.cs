namespace MassTransit.AmazonSqsTransport
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.SimpleNotificationService;
    using Amazon.SimpleNotificationService.Model;


    public class TopicInfo :
        IAsyncDisposable
    {
        readonly PublishBatcher _batchPublisher;
        bool _disposed;

        public TopicInfo(string entityName, string arn, IAmazonSimpleNotificationService client, CancellationToken cancellationToken)
        {
            EntityName = entityName;
            Arn = arn;

            _batchPublisher = new PublishBatcher(client, arn, cancellationToken);
        }

        public string EntityName { get; }
        public string Arn { get; }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            _disposed = true;

            LogContext.Debug?.Log("Disposing publisher: {EntityName}", EntityName);

            await _batchPublisher.DisposeAsync().ConfigureAwait(false);
        }

        public Task Publish(PublishBatchRequestEntry entry, CancellationToken cancellationToken)
        {
            return _batchPublisher.Execute(entry, cancellationToken);
        }
    }
}
