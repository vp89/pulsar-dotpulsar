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
using DotPulsar.Internal.PulsarApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class PartitionedTopicConsumer : IConsumer
    {
        public List<string> Topics { get; }
        
        private readonly Guid _correlationId;
        private readonly IRegisterEvent _eventRegister;
        private List<IConsumerChannel> _channels;
        private readonly CommandAck _cachedCommandAck;
        private readonly IExecute _executor;
        private readonly IStateChanged<ConsumerState> _state;
        private int _isDisposed;

        public PartitionedTopicConsumer(
            Guid correlationId,
            IRegisterEvent eventRegister,
            List<string> topics,
            IExecute executor,
            IStateChanged<ConsumerState> state)
        {
            _correlationId = correlationId;
            _eventRegister = eventRegister;
            Topics = topics;

            _channels = new List<IConsumerChannel>(topics.Count);

            foreach (var _ in topics)
            {
                _channels.Add(new NotReadyChannel());
            }
            
            _executor = executor;
            _state = state;
            _cachedCommandAck = new CommandAck();
            _isDisposed = 0;

            _eventRegister.Register(new ConsumerCreated(_correlationId, this));
        }

        public async ValueTask<ConsumerState> StateChangedTo(ConsumerState state, CancellationToken cancellationToken)
            => await _state.StateChangedTo(state, cancellationToken);

        public async ValueTask<ConsumerState> StateChangedFrom(ConsumerState state, CancellationToken cancellationToken)
            => await _state.StateChangedFrom(state, cancellationToken);

        public bool IsFinalState() => _state.IsFinalState();

        public bool IsFinalState(ConsumerState state) => _state.IsFinalState(state);

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
                return;

            _eventRegister.Register(new ConsumerDisposed(_correlationId, this));
            
            foreach (var channel in _channels)
            {
                await channel.DisposeAsync();
            }
        }

        public async IAsyncEnumerable<Message> Messages([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            while (!cancellationToken.IsCancellationRequested)
            {
                // TODO can this be distributed properly to receive in correct order?
                foreach (var channel in _channels)
                {
                    yield return await _executor.Execute(() => channel.Receive(cancellationToken), cancellationToken);    
                }
            }
        }

        public async ValueTask Acknowledge(Message message, CancellationToken cancellationToken)
            => await Acknowledge(message.MessageId.Data, CommandAck.AckType.Individual, cancellationToken);

        public async ValueTask Acknowledge(MessageId messageId, CancellationToken cancellationToken)
            => await Acknowledge(messageId.Data, CommandAck.AckType.Individual, cancellationToken);

        public async ValueTask AcknowledgeCumulative(Message message, CancellationToken cancellationToken)
            => await Acknowledge(message.MessageId.Data, CommandAck.AckType.Cumulative, cancellationToken);

        public async ValueTask AcknowledgeCumulative(MessageId messageId, CancellationToken cancellationToken)
            => await Acknowledge(messageId.Data, CommandAck.AckType.Cumulative, cancellationToken);

        public async ValueTask Unsubscribe(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            
            foreach (var channel in _channels)
            {
                _ = await _executor.Execute(() => channel.Send(new CommandUnsubscribe()), cancellationToken);    
            }
        }

        public async ValueTask Seek(MessageId messageId, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            var seek = new CommandSeek { MessageId = messageId.Data };
            var partition = messageId.Partition;
            
            _ = await _executor.Execute(() => _channels[partition].Send(seek), cancellationToken);
            return;
        }

        public async ValueTask<MessageId> GetLastMessageId(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            MessageIdData lastMessageId = null;
            
            foreach (var channel in _channels)
            {
                var response = await _executor.Execute(() => channel.Send(new CommandGetLastMessageId()), cancellationToken);

                if (response.LastMessageId.EntryId > lastMessageId?.EntryId)
                {
                    lastMessageId = response.LastMessageId;
                }
            }
            
            return new MessageId(lastMessageId);
        }

        private async ValueTask Acknowledge(MessageIdData messageIdData, CommandAck.AckType ackType, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            
            await _executor.Execute(() =>
            {
                _cachedCommandAck.Type = ackType;
                _cachedCommandAck.MessageIds.Clear();
                _cachedCommandAck.MessageIds.Add(messageIdData);

                var partition = messageIdData.Partition;
                return _channels[partition].Send(_cachedCommandAck);
            }, cancellationToken);
        }

        public void SetChannels(List<IConsumerChannel> channels)
        {
            ThrowIfDisposed();
            _channels = channels;
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed != 0)
                throw new ObjectDisposedException(GetType().FullName);
        }
    }
}
