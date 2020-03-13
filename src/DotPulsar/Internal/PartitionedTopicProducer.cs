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

using DotPulsar.Abstractions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Events;
using Murmur;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class PartitionedTopicProducer : IProducer
    {
        public List<string> Topics { get; }

        private readonly Guid _correlationId;
        private readonly IRegisterEvent _eventRegister;
        private List<IProducerChannel> _channels;
        private readonly IExecute _executor;
        private readonly IStateChanged<ProducerState> _state;
        private readonly ProducerOptions _options;
        private readonly HashAlgorithm _hashAlgorithm;
        private int _isDisposed;
        private int _singlePartitionModeIndex = 0;
        private int _roundRobinPartitionModeIndex = 0;
        private readonly object _roundRobinLock = new object();

        public PartitionedTopicProducer(
            Guid correlationId,
            IRegisterEvent registerEvent,
            List<string> topics,
            IExecute executor,
            IStateChanged<ProducerState> state,
            ProducerOptions options)
        {
            _correlationId = correlationId;
            _eventRegister = registerEvent;
            Topics = topics;

            _channels = new List<IProducerChannel>(topics.Count);

            foreach (var _ in topics)
            {
                _channels.Add(new NotReadyChannel());
            }

            _executor = executor;
            _state = state;
            _options = options;

            _isDisposed = 0;
            _hashAlgorithm = MurmurHash.Create32();
            _eventRegister.Register(new ProducerCreated(_correlationId, this));
        }

        public async ValueTask<ProducerState> StateChangedTo(ProducerState state, CancellationToken cancellationToken)
            => await _state.StateChangedTo(state, cancellationToken);

        public async ValueTask<ProducerState> StateChangedFrom(ProducerState state, CancellationToken cancellationToken)
            => await _state.StateChangedFrom(state, cancellationToken);

        public bool IsFinalState() => _state.IsFinalState();

        public bool IsFinalState(ProducerState state) => _state.IsFinalState(state);

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
                return;

            _eventRegister.Register(new ProducerDisposed(_correlationId, this));

            foreach (var channel in _channels)
            {
                await channel.DisposeAsync();
            }
        }

        public async ValueTask<MessageId> Send(
            byte[] data,
            CancellationToken cancellationToken)
        {
            return await Send(new ReadOnlySequence<byte>(data), cancellationToken);
        }

        public async ValueTask<MessageId> Send(
            ReadOnlyMemory<byte> data,
            CancellationToken cancellationToken)
        {
            return await Send(new ReadOnlySequence<byte>(data), cancellationToken);
        }

        public async ValueTask<MessageId> Send(
            ReadOnlySequence<byte> data,
            CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            var chosen = ChooseChannel();
            var response = await _executor.Execute(() => _channels[chosen].Send(data, cancellationToken), cancellationToken);
            return new MessageId(response.MessageId);
        }

        public async ValueTask<MessageId> Send(
            MessageMetadata metadata,
            byte[] data,
            CancellationToken cancellationToken)
        {
            return await Send(metadata, new ReadOnlySequence<byte>(data), cancellationToken);
        }

        public async ValueTask<MessageId> Send(
            MessageMetadata metadata,
            ReadOnlyMemory<byte> data,
            CancellationToken cancellationToken)
        {
            return await Send(metadata, new ReadOnlySequence<byte>(data), cancellationToken);
        }

        public async ValueTask<MessageId> Send(
            MessageMetadata metadata,
            ReadOnlySequence<byte> data,
            CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            int chosen = ChooseChannel(metadata?.Key);
            var response = await _executor.Execute(() => _channels[chosen].Send(metadata.Metadata, data, cancellationToken), cancellationToken);
            return new MessageId(response.MessageId);
        }

        public void SetChannels(List<IProducerChannel> channels)
        {
            ThrowIfDisposed();

            if (_options.MessageRoutingMode == MessageRoutingMode.SinglePartition)
            {
                _singlePartitionModeIndex = new Random().Next(0, channels.Count);
            }

            _channels = channels;
        }

        private int ChooseChannel(string partitionKey = default)
        {
            if (!string.IsNullOrEmpty(partitionKey))
            {
                var partitionKeyBytes = Encoding.UTF8.GetBytes(partitionKey);
                var hashBytes = _hashAlgorithm.ComputeHash(partitionKeyBytes);
                var hashNumber = BitConverter.ToInt32(hashBytes, 0);
                return Math.Abs(hashNumber % _channels.Count);
            }

            switch (_options.MessageRoutingMode)
            {
                case MessageRoutingMode.RoundRobinPartition:
                    lock (_roundRobinLock)
                    {
                        _roundRobinPartitionModeIndex++;
                        if (_roundRobinPartitionModeIndex >= _channels.Count)
                        {
                            _roundRobinPartitionModeIndex = 0;
                        }

                        return _roundRobinPartitionModeIndex;
                    }
                case MessageRoutingMode.SinglePartition:
                    return _singlePartitionModeIndex;
                default:
                    throw new NotImplementedException();
            }
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed != 0)
                throw new ObjectDisposedException(nameof(Producer));
        }
    }
}
