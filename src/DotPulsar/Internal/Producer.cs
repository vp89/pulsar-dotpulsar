﻿/*
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
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class Producer : IProducer
    {
        public List<string> Topics { get; }

        private readonly Guid _correlationId;
        private readonly IRegisterEvent _eventRegister;
        private IProducerChannel _channel;
        private readonly IExecute _executor;
        private readonly IStateChanged<ProducerState> _state;
        private int _isDisposed;

        public Producer(
            Guid correlationId,
            IRegisterEvent registerEvent,
            string topic,
            IProducerChannel initialChannel,
            IExecute executor,
            IStateChanged<ProducerState> state)
        {
            _correlationId = correlationId;
            _eventRegister = registerEvent;
            Topics = new List<string>() { topic };
            _channel = initialChannel;
            _executor = executor;
            _state = state;
            _isDisposed = 0;

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
            await _channel.DisposeAsync();
        }

        public async ValueTask<MessageId> Send(byte[] data, CancellationToken cancellationToken)
            => await Send(new ReadOnlySequence<byte>(data), cancellationToken);

        public async ValueTask<MessageId> Send(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
            => await Send(new ReadOnlySequence<byte>(data), cancellationToken);

        public async ValueTask<MessageId> Send(ReadOnlySequence<byte> data, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            var response = await _executor.Execute(() => _channel.Send(data, cancellationToken), cancellationToken);
            return new MessageId(response.MessageId);
        }

        public async ValueTask<MessageId> Send(MessageMetadata metadata, byte[] data, CancellationToken cancellationToken)
            => await Send(metadata, new ReadOnlySequence<byte>(data), cancellationToken);

        public async ValueTask<MessageId> Send(MessageMetadata metadata, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
            => await Send(metadata, new ReadOnlySequence<byte>(data), cancellationToken);

        public async ValueTask<MessageId> Send(MessageMetadata metadata, ReadOnlySequence<byte> data, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            var response = await _executor.Execute(() => _channel.Send(metadata.Metadata, data, cancellationToken), cancellationToken);
            return new MessageId(response.MessageId);
        }

        public void SetChannels(List<IProducerChannel> channels)
        {
            ThrowIfDisposed();

            if (channels.Count != 1)
                throw new Exception($"Expected 1 channel passed in to {nameof(Producer)} but received {channels.Count}");
            
            _channel = channels[0];
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed != 0)
                throw new ObjectDisposedException(nameof(Producer));
        }
    }
}
