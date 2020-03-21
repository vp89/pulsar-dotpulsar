/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.PulsarApi;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class ConsumerChannelFactory : IConsumerChannelFactory
    {
        private readonly Guid _correlationId;
        private readonly IRegisterEvent _eventRegister;
        private readonly IConnectionPool _connectionPool;
        private readonly IExecute _executor;
        private readonly CommandSubscribe _subscribe;
        private readonly uint _messagePrefetchCount;
        private readonly BatchHandler _batchHandler;
        private readonly ConsumerOptions _options;

        public ConsumerChannelFactory(
            Guid correlationId,
            IRegisterEvent eventRegister,
            IConnectionPool connectionPool,
            IExecute executor,
            ConsumerOptions options)
        {
            _correlationId = correlationId;
            _eventRegister = eventRegister;
            _connectionPool = connectionPool;
            _executor = executor;
            _messagePrefetchCount = options.MessagePrefetchCount;
            _options = options;
            
            _batchHandler = new BatchHandler(true);
        }

        public async Task<IConsumerChannel> Create(string topic, CancellationToken cancellationToken)
            => await _executor.Execute(() => GetChannel(topic, cancellationToken), cancellationToken);

        private async ValueTask<IConsumerChannel> GetChannel(string topic, CancellationToken cancellationToken)
        {
            var subscribe = new CommandSubscribe
            {
                ConsumerName = _options.ConsumerName,
                initialPosition = (CommandSubscribe.InitialPosition)_options.InitialPosition,
                PriorityLevel = _options.PriorityLevel,
                ReadCompacted = _options.ReadCompacted,
                Subscription = _options.SubscriptionName,
                Topic = _options.Topic,
                Type = (CommandSubscribe.SubType)_options.SubscriptionType
            };
            
            var connection = await _connectionPool.FindConnectionForTopic(topic, cancellationToken);
            var messageQueue = new AsyncQueue<MessagePackage>();
            var channel = new Channel(_correlationId, _eventRegister, messageQueue);
            var response = await connection.Send(subscribe, channel);
            return new ConsumerChannel(response.ConsumerId, _messagePrefetchCount, messageQueue, connection, _batchHandler);
        }
    }
}
