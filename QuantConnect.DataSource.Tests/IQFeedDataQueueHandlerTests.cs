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
using System.Linq;
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
    [TestFixture, Explicit("This tests require a IQFeed credentials.")]
    public class IQFeedDataQueueHandlerTests
    {
        private IQFeedDataQueueHandler _iqFeed;

        private const int _minimumReturnDataAmount = 5;

        [SetUp]
        public void SetUp()
        {
            _iqFeed = new IQFeedDataQueueHandler();
        }

        [TearDown]
        public void TearDown()
        {
            _iqFeed.Dispose();
        }

        private static IEnumerable<TestCaseData> SubscribeTestCaseData
        {
            get
            {
                yield return new TestCaseData(Symbols.AAPL);
                yield return new TestCaseData(Symbols.SPY);

                // Not supported.
                // yield return new TestCaseData(Symbol.Create("SPX.XO", SecurityType.Index, Market.CBOE)); // S&P 500 INDEX
                // yield return new TestCaseData(Symbol.CreateFuture(Futures.Indices.SP500EMini, Market.CME, new DateTime(2024, 3, 21))); // E-MINI S&P 500 ESG INDEX March 2024
            }
        }

        [TestCaseSource(nameof(SubscribeTestCaseData))]
        public void SubscribeOnTickData(Symbol symbol)
        {
            var autoResetEvent = new AutoResetEvent(false);
            var configs = GetSubscriptionDataConfigs(symbol, Resolution.Tick);

            var tickDataReceived = new Dictionary<TickType, int> { { TickType.Trade, 0 }, { TickType.Quote, 0 } };

            Action<BaseData> callback = (dataPoint) =>
            {
                if (dataPoint == null)
                {
                    return;
                }

                if (dataPoint is Tick tick)
                {
                    tickDataReceived[tick.TickType] += 1;

                    if (tickDataReceived.All(type => type.Value >= _minimumReturnDataAmount))
                    {
                        autoResetEvent.Set();
                    }
                }
            };

            SubscribeOnData(configs, callback, autoResetEvent);

            foreach (var tickData in tickDataReceived)
            {
                Assert.Greater(tickData.Value, _minimumReturnDataAmount);
            }
        }

        [TestCaseSource(nameof(SubscribeTestCaseData))]
        public void SubscribeOnSecondData(Symbol symbol)
        {
            var autoResetEvent = new AutoResetEvent(false);
            var configs = GetSubscriptionDataConfigs(symbol, Resolution.Second);

            var secondDataReceived = new Dictionary<Type, int> { { typeof(TradeBar), 0 }, { typeof(QuoteBar), 0 } };

            Action<BaseData> callback = (dataPoint) =>
            {
                if (dataPoint == null)
                {
                    return;
                }

                switch (dataPoint)
                {
                    case TradeBar _:
                        secondDataReceived[typeof(TradeBar)] += 1;
                        break;
                    case QuoteBar _:
                        secondDataReceived[typeof(QuoteBar)] += 1;
                        break;
                }

                if (secondDataReceived.All(type => type.Value >= _minimumReturnDataAmount))
                {
                    autoResetEvent.Set();
                }
            };

            SubscribeOnData(configs, callback, autoResetEvent);

            foreach (var tickData in secondDataReceived)
            {
                Assert.Greater(tickData.Value, _minimumReturnDataAmount);
            }
        }


        public void SubscribeOnData(SubscriptionDataConfig[] configs, Action<BaseData> callback, AutoResetEvent autoResetEvent)
        {
            var cancellationTokenSource = new CancellationTokenSource();

            foreach (var config in configs)
            {
                ProcessFeed(_iqFeed.Subscribe(config, (sender, args) =>
                {
                    var dataPoint = ((NewDataAvailableEventArgs)args).DataPoint;
                    Log.Trace($"{dataPoint}. Time span: {dataPoint.Time} - {dataPoint.EndTime}");
                }), callback);
            }

            Assert.IsTrue(autoResetEvent.WaitOne(TimeSpan.FromSeconds(60), cancellationTokenSource.Token));

            foreach (var config in configs)
            {
                _iqFeed.Unsubscribe(config);
            }

            Thread.Sleep(TimeSpan.FromSeconds(1));
            cancellationTokenSource.Cancel();
        }

        private SubscriptionDataConfig[] GetSubscriptionDataConfigs(Symbol symbol, Resolution resolution)
        {
            SubscriptionDataConfig[] configs;
            if (resolution == Resolution.Tick)
            {
                var tradeConfig = new SubscriptionDataConfig(GetSubscriptionDataConfig<Tick>(symbol, resolution),
                    tickType: TickType.Trade);
                var quoteConfig = new SubscriptionDataConfig(GetSubscriptionDataConfig<Tick>(symbol, resolution),
                    tickType: TickType.Quote);
                configs = new[] { tradeConfig, quoteConfig };
            }
            else
            {
                configs = new[]
                {
                    GetSubscriptionDataConfig<QuoteBar>(symbol, resolution),
                    GetSubscriptionDataConfig<TradeBar>(symbol, resolution)
                };
            }
            return configs;
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
