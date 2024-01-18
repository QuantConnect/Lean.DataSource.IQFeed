/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

using System;
using NUnit.Framework;
using System.Threading;
using QuantConnect.Data;
using QuantConnect.Tests;
using QuantConnect.Logging;
using System.Threading.Tasks;
using QuantConnect.Data.Market;
using System.Collections.Generic;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;

namespace QuantConnect.DataSource.Tests
{
    [TestFixture]
    public class IQFeedDataQueueHandlerTests
    {
        [Test]
        public void IsConnectedReturnsTrue()
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var autoResetEvent = new AutoResetEvent(false);
            using var iqFeed = new IQFeedDataQueueHandler();

            Assert.IsTrue(iqFeed.IsConnected);

            var dataFromEnumerator = new List<TradeBar>();
            var dataFromEventHandler = new List<TradeBar>();

            Action<BaseData> callback = (dataPoint) =>
            {
                if (dataPoint == null)
                {
                    return;
                }

                dataFromEnumerator.Add((TradeBar)dataPoint);
            };

            var config = GetSubscriptionDataConfig<TradeBar>(Symbols.SPY, Resolution.Tick);

            ProcessFeed(iqFeed.Subscribe(config, (sender, args) =>
            {
                var dataPoint = ((NewDataAvailableEventArgs)args).DataPoint;
                if (dataPoint == null)
                {
                    return;
                }

                dataFromEventHandler.Add((TradeBar)dataPoint);
                Log.Trace($"{dataPoint}. Time span: {dataPoint.Time} - {dataPoint.EndTime}");

                if(dataFromEventHandler.Count > 10)
                {
                    autoResetEvent.Set();
                }

            }), callback);

            autoResetEvent.WaitOne(TimeSpan.FromSeconds(60), cancellationTokenSource.Token);
            Assert.Greater(dataFromEnumerator.Count, 0);
            Assert.Greater(dataFromEventHandler.Count, 0);

        }

        [Test]
        public void StartingRun()
        {
            var iq = new IQFeedDataQueueHandler();

            var unsubscribed = false;

            var dataFromEnumerator = new List<TradeBar>();
            var dataFromEventHandler = new List<TradeBar>();

            Action<BaseData> callback = (dataPoint) =>
            {
                if (dataPoint == null)
                {
                    return;
                }

                dataFromEnumerator.Add((TradeBar)dataPoint);

                if (unsubscribed)
                {
                    Assert.Fail("Should not receive data for unsubscribed symbols");
                }
            };

            var config = GetSubscriptionDataConfig<TradeBar>(Symbols.SPY, Resolution.Minute);

            ProcessFeed(iq.Subscribe(config, (sender, args) =>
            {
                var dataPoint = ((NewDataAvailableEventArgs)args).DataPoint;
                dataFromEventHandler.Add((TradeBar)dataPoint);
                Log.Trace($"{dataPoint}. Time span: {dataPoint.Time} - {dataPoint.EndTime}");
            }), callback);
        }

        private SubscriptionDataConfig GetSubscriptionDataConfig<T>(Symbol symbol, Resolution resolution)
        {
            return new SubscriptionDataConfig(
                typeof(T),
                symbol,
                resolution,
                TimeZones.Utc,
                TimeZones.Utc,
                true,
                true,
                false);
        }


        private void ProcessFeed(IEnumerator<BaseData> enumerator, Action<BaseData> callback = null)
        {
            Task.Run(() =>
            {
                try
                {
                    while (enumerator.MoveNext())
                    {
                        var tick = enumerator.Current;
                        callback?.Invoke(tick);
                    }
                }
                catch (AssertionException)
                {
                    throw;
                }
                catch (Exception err)
                {
                    Log.Error(err.Message);
                }
            });
        }
    }
}
