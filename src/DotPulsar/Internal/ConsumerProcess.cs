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

using DotPulsar.Internal.Abstractions;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DotPulsar.Abstractions;

namespace DotPulsar.Internal
{
    public sealed class ConsumerProcess : Process
    {
        private readonly IStateManager<ConsumerState> _stateManager;
        private readonly IConsumerChannelFactory _factory;
        private readonly IConsumer _consumer;
        private readonly bool _isFailoverSubscription;

        public ConsumerProcess(
            Guid correlationId,
            IStateManager<ConsumerState> stateManager,
            IConsumerChannelFactory factory,
            IConsumer consumer,
            bool isFailoverSubscription) : base(correlationId)
        {
            _stateManager = stateManager;
            _factory = factory;
            _consumer = consumer;
            _isFailoverSubscription = isFailoverSubscription;
        }

        public async override ValueTask DisposeAsync()
        {
            _stateManager.SetState(ConsumerState.Closed);
            CancellationTokenSource.Cancel();
            await _consumer.DisposeAsync();
        }

        protected override void CalculateState()
        {
            if (_consumer.IsFinalState())
                return;

            if (ExecutorState == ExecutorState.Faulted)
            {
                _stateManager.SetState(ConsumerState.Faulted);
                return;
            }

            switch (ChannelState)
            {
                case ChannelState.Active:
                    _stateManager.SetState(ConsumerState.Active);
                    return;
                case ChannelState.Inactive:
                    _stateManager.SetState(ConsumerState.Inactive);
                    return;
                case ChannelState.ClosedByServer:
                case ChannelState.Disconnected:
                    _stateManager.SetState(ConsumerState.Disconnected);
                    SetupChannel();
                    return;
                case ChannelState.Connected:
                    if (!_isFailoverSubscription)
                        _stateManager.SetState(ConsumerState.Active);
                    return;
                case ChannelState.ReachedEndOfTopic:
                    _stateManager.SetState(ConsumerState.ReachedEndOfTopic);
                    return;
                case ChannelState.Unsubscribed:
                    _stateManager.SetState(ConsumerState.Unsubscribed);
                    return;
            }
        }

        private async void SetupChannel()
        {
            var channels = new List<IConsumerChannel>();

            try
            {
                foreach (var topic in _consumer.Topics)
                {
                    channels.Add(await _factory.Create(topic, CancellationTokenSource.Token));
                }

                _consumer.SetChannels(channels);
            }
            catch
            {
                if (channels != null)
                {
                    foreach (var channel in channels)
                    {
                        await channel.DisposeAsync();
                    }
                }
            }
        }
    }
}
