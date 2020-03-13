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

using DotPulsar.Exceptions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.PulsarApi;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class ConnectionPool : IConnectionPool
    {
        private readonly AsyncLock _lock;
        private readonly CommandConnect _commandConnect;
        private readonly Uri _serviceUrl;
        private readonly Connector _connector;
        private readonly EncryptionPolicy _encryptionPolicy;
        private readonly ConcurrentDictionary<Uri, Connection> _connections;

        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly Task _closeInactiveConnections;

        public ConnectionPool(CommandConnect commandConnect, Uri serviceUrl, Connector connector, EncryptionPolicy encryptionPolicy, TimeSpan closeInactiveConnectionsInterval)
        {
            _lock = new AsyncLock();
            _commandConnect = commandConnect;
            _serviceUrl = serviceUrl;
            _connector = connector;
            _encryptionPolicy = encryptionPolicy;
            _connections = new ConcurrentDictionary<Uri, Connection>();
            _cancellationTokenSource = new CancellationTokenSource();
            _closeInactiveConnections = CloseInactiveConnections(closeInactiveConnectionsInterval, _cancellationTokenSource.Token);
        }

        public async ValueTask DisposeAsync()
        {
            _cancellationTokenSource.Cancel();
            await _closeInactiveConnections;

            await _lock.DisposeAsync();

            foreach (var serviceUrl in _connections.Keys.ToArray())
            {
                await DisposeConnection(serviceUrl);
            }
        }

        public async ValueTask<List<string>> GetTopics(string topic, CancellationToken cancellationToken)
        {
            var partitionedMetadataCommand = new CommandPartitionedTopicMetadata
            {
                Topic = topic
            };

            var connection = await GetConnection(_serviceUrl, _serviceUrl, cancellationToken);
            var response = await connection.Send(partitionedMetadataCommand, cancellationToken);
            response.Expect(BaseCommand.Type.PartitionedMetadataResponse);

            var partitionCount = response.PartitionMetadataResponse.Partitions;

            if (partitionCount == 0)
            {
                return new List<string>() { topic };
            }

            var partitionedTopicNames = new List<string>((int)partitionCount);

            for (int i = 0; i < partitionCount; i++)
            {
                partitionedTopicNames.Add($"{topic}-partition-{i}");
            }

            return partitionedTopicNames;
        }

        public async ValueTask<IConnection> FindConnectionForTopic(string topic, CancellationToken cancellationToken)
        {
            var lookup = new CommandLookupTopic
            {
                Topic = topic,
                Authoritative = false
            };

            var logicalUrl = _serviceUrl;
            var physicalUrl = _serviceUrl;

            while (true)
            {
                var connection = await GetConnection(logicalUrl, physicalUrl, cancellationToken);
                var response = await connection.Send(lookup, cancellationToken);

                response.Expect(BaseCommand.Type.LookupResponse);

                if (response.LookupTopicResponse.Response == CommandLookupTopicResponse.LookupType.Failed)
                    response.LookupTopicResponse.Throw();

                lookup.Authoritative = response.LookupTopicResponse.Authoritative;

                logicalUrl = new Uri(GetBrokerServiceUrl(response.LookupTopicResponse));

                if (response.LookupTopicResponse.Response == CommandLookupTopicResponse.LookupType.Redirect || !response.LookupTopicResponse.Authoritative)
                    continue;

                return await GetConnection(logicalUrl, physicalUrl, cancellationToken);
            }
        }

        private string GetBrokerServiceUrl(CommandLookupTopicResponse response)
        {
            var hasBrokerServiceUrl = !string.IsNullOrEmpty(response.BrokerServiceUrl);
            var hasBrokerServiceUrlTls = !string.IsNullOrEmpty(response.BrokerServiceUrlTls);

            switch (_encryptionPolicy)
            {
                case EncryptionPolicy.EnforceEncrypted:
                    if (!hasBrokerServiceUrlTls)
                        throw new ConnectionSecurityException("Cannot enforce encrypted connections. The lookup topic response from broker gave no secure alternative.");
                    return response.BrokerServiceUrlTls;
                case EncryptionPolicy.EnforceUnencrypted:
                    if (!hasBrokerServiceUrl)
                        throw new ConnectionSecurityException("Cannot enforce unencrypted connections. The lookup topic response from broker gave no unsecure alternative.");
                    return response.BrokerServiceUrl;
                case EncryptionPolicy.PreferEncrypted:
                    return hasBrokerServiceUrlTls ? response.BrokerServiceUrlTls : response.BrokerServiceUrl;
                case EncryptionPolicy.PreferUnencrypted:
                default:
                    return hasBrokerServiceUrl ? response.BrokerServiceUrl : response.BrokerServiceUrlTls;
            }
        }

        // The logical Url differs from the physical Url when you are
        // connecting through a Pulsar proxy. We create 1 physical connection to
        // the Proxy for each logical broker connection we require according to
        // the topic lookup.
        private async ValueTask<Connection> GetConnection(Uri logicalUrl, Uri physicalUrl, CancellationToken cancellationToken)
        {
            using (await _lock.Lock(cancellationToken))
            {
                if (_connections.TryGetValue(logicalUrl, out Connection connection))
                    return connection;

                return await EstablishNewConnection(logicalUrl, physicalUrl, cancellationToken);
            }
        }

        private async Task<Connection> EstablishNewConnection(Uri logicalUrl, Uri physicalUrl, CancellationToken cancellationToken)
        {
            var stream = await _connector.Connect(physicalUrl);
            var connection = new Connection(new PulsarStream(stream));
            DotPulsarEventSource.Log.ConnectionCreated();
            _connections[logicalUrl] = connection;
            _ = connection.ProcessIncommingFrames(cancellationToken).ContinueWith(t => DisposeConnection(logicalUrl));

            if (logicalUrl != physicalUrl)
            {
                // DirectProxyHandler expects the Url with no scheme provided
                _commandConnect.ProxyToBrokerUrl = $"{logicalUrl.Host}:{logicalUrl.Port}";
            }

            var response = await connection.Send(_commandConnect, cancellationToken);
            response.Expect(BaseCommand.Type.Connected);

            _commandConnect.ResetProxyToBrokerUrl(); // reset so we can re-use this object

            return connection;
        }

        private async ValueTask DisposeConnection(Uri logicalUrl)
        {
            if (_connections.TryRemove(logicalUrl, out var connection))
            {
                await connection.DisposeAsync();
                DotPulsarEventSource.Log.ConnectionDisposed();
            }
        }

        private async Task CloseInactiveConnections(TimeSpan interval, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(interval, cancellationToken);

                    using (await _lock.Lock(cancellationToken))
                    {
                        var serviceUrls = _connections.Keys;
                        foreach (var serviceUrl in serviceUrls)
                        {
                            var connection = _connections[serviceUrl];
                            if (connection is null)
                                continue;
                            if (!await connection.HasChannels(cancellationToken))
                                await DisposeConnection(serviceUrl);
                        }
                    }
                }
                catch { }
            }
        }
    }
}
