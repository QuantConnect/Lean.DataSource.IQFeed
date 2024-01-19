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
                yield return new TestCaseData(Symbol.Create("SMCI", SecurityType.Equity, Market.USA));
                yield return new TestCaseData(Symbol.Create("IRBT", SecurityType.Equity, Market.USA));
                // yield return new TestCaseData(Symbols.SPY);

                // Not supported.
                // yield return new TestCaseData(Symbol.Create("SPX.XO", SecurityType.Index, Market.CBOE)); // S&P 500 INDEX
                // yield return new TestCaseData(Symbol.CreateFuture(Futures.Indices.SP500EMini, Market.CME, new DateTime(2024, 3, 21))); // E-MINI S&P 500 ESG INDEX March 2024
            }
        }

        [TestCaseSource(nameof(SubscribeTestCaseData))]
        public void SubscribeOnTickData(Symbol symbol)
        {
            var minimumReturnDataAmount = 5;
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

                    switch (tick.TickType)
                    {
                        case TickType.Trade:
                            AssertAllGreaterThanZero(tick.Price, tick.Quantity);
                            break;
                        case TickType.Quote:
                            AssertAllGreaterThanZero(tick.BidPrice, tick.AskPrice, tick.BidSize, tick.AskSize);
                            break;
                    }

                    if (tickDataReceived.All(type => type.Value >= minimumReturnDataAmount))
                    {
                        autoResetEvent.Set();
                    }
                }
            };

            SubscribeOnDataByConfigs(configs, callback, autoResetEvent);

            foreach (var tickData in tickDataReceived)
            {
                Log.Trace($"{nameof(SubscribeOnTickData)}: Return {tickData.Key} = {tickData.Value}");
                Assert.GreaterOrEqual(tickData.Value, minimumReturnDataAmount);
            }
        }

        [TestCaseSource(nameof(SubscribeTestCaseData))]
        public void SubscribeOnSecondData(Symbol symbol)
        {
            SubscribeOnData(symbol, Resolution.Tick, 3);
        }

        [TestCaseSource(nameof(SubscribeTestCaseData))]
        public void SubscribeOnMinuteData(Symbol symbol)
        {
            SubscribeOnData(symbol, Resolution.Minute, maxWaitResponseTimeSecond: 80);
        }

        private void SubscribeOnData(Symbol symbol, Resolution resolution, int minimumReturnDataAmount = 1, int maxWaitResponseTimeSecond = 60)
        {
            var autoResetEvent = new AutoResetEvent(false);
            var configs = GetSubscriptionDataConfigs(symbol, resolution);

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

                if (secondDataReceived.All(type => type.Value >= minimumReturnDataAmount))
                {
                    autoResetEvent.Set();
                }
            };

            SubscribeOnDataByConfigs(configs, callback, autoResetEvent, maxWaitResponseTimeSecond);

            foreach (var tickData in secondDataReceived)
            {
                Log.Trace($"{nameof(SubscribeOnSecondData)}: Return {tickData.Key} = {tickData.Value}");
                Assert.GreaterOrEqual(tickData.Value, minimumReturnDataAmount);
            }
        }

        private void SubscribeOnDataByConfigs(SubscriptionDataConfig[] configs, Action<BaseData> callback, AutoResetEvent autoResetEvent, int maxWaitResponseTimeSecond = 60)
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

            autoResetEvent.WaitOne(TimeSpan.FromSeconds(maxWaitResponseTimeSecond), cancellationTokenSource.Token);

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

        /// <summary>
        /// Asserts that all decimal values in the specified list are greater than zero.
        /// </summary>
        /// <param name="list">The list of decimal values to be asserted.</param>
        /// <remarks>
        /// This method uses the NUnit.Framework.Assert.Greater method to check that each decimal value in the list
        /// is greater than zero. If any value is less than or equal to zero, an assertion failure will be raised.
        /// </remarks>
        /// <example>
        /// The following example demonstrates the usage of AssertAllGreaterThanZero:
        /// <code>
        /// decimal[] values = { 1.5m, 2.3m, 3.8m };
        /// AssertAllGreaterThanZero(values);
        /// </code>
        /// </example>
        private static void AssertAllGreaterThanZero(params decimal[] list)
        {
            foreach (var item in list)
            {
                Assert.Greater(item, 0);
            }
        }
    }
}
