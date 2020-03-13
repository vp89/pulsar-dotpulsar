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
    public sealed class ProducerChannelFactory : IProducerChannelFactory
    {
        private readonly Guid _correlationId;
        private readonly IRegisterEvent _eventRegister;
        private readonly IConnectionPool _connectionPool;
        private readonly IExecute _executor;
        private readonly SequenceId _sequenceId;

        public ProducerChannelFactory(
            Guid correlationId,
            IRegisterEvent eventRegister,
            IConnectionPool connectionPool,
            IExecute executor,
            ProducerOptions options)
        {
            _correlationId = correlationId;
            _eventRegister = eventRegister;
            _connectionPool = connectionPool;
            _executor = executor;
            _sequenceId = new SequenceId(options.InitialSequenceId);
        }

        public async Task<IProducerChannel> Create(string topic, CancellationToken cancellationToken)
            => await _executor.Execute(() => GetChannel(topic, cancellationToken), cancellationToken);

        private async ValueTask<IProducerChannel> GetChannel(string topic, CancellationToken cancellationToken)
        {
            var commandProducer = new CommandProducer { Topic = topic };
            var connection = await _connectionPool.FindConnectionForTopic(topic, cancellationToken);
            var channel = new Channel(_correlationId, _eventRegister, new AsyncQueue<MessagePackage>());
            var response = await connection.Send(commandProducer, channel, cancellationToken);
            return new ProducerChannel(response.ProducerId, response.ProducerName, _sequenceId, connection);
        }
    }
}
