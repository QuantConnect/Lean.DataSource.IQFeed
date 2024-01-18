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
*/

using System;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Tests;
using QuantConnect.Securities;
using System.Collections.Generic;
using QuantConnect.Data.Market;
using static Plotly.NET.StyleParam;

namespace QuantConnect.DataSource.Tests
{
    [TestFixture]
    public class IQFeedHistoryProviderTests
    {
        private IQFeedDataQueueHandler _historyProvider;

        [SetUp]
        public void SetUp()
        {
            _historyProvider = new IQFeedDataQueueHandler();
            _historyProvider.Initialize(new HistoryProviderInitializeParameters(null, null, null, null, null, null, null, false, null, null));
        }

        [TearDown]
        public void TearDown()
        {
            _historyProvider.Dispose();
        }

        private static IEnumerable<TestCaseData> TestParameters
        {
            get
            {
                var AAPL = Symbols.AAPL;

                yield return new TestCaseData(AAPL, Resolution.Tick, TickType.Trade, TimeSpan.FromMinutes(5));
                yield return new TestCaseData(AAPL, Resolution.Second, TickType.Trade, TimeSpan.FromMinutes(10));
                yield return new TestCaseData(AAPL, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(10));
                yield return new TestCaseData(AAPL, Resolution.Hour, TickType.Trade, TimeSpan.FromDays(180));
                yield return new TestCaseData(AAPL, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(365));

                yield return new TestCaseData(AAPL, Resolution.Tick, TickType.Quote, TimeSpan.FromMinutes(5));
                yield return new TestCaseData(AAPL, Resolution.Second, TickType.Quote, TimeSpan.FromMinutes(10));
                yield return new TestCaseData(AAPL, Resolution.Minute, TickType.Quote, TimeSpan.FromDays(10));
                yield return new TestCaseData(AAPL, Resolution.Hour, TickType.Quote, TimeSpan.FromDays(180));
                yield return new TestCaseData(AAPL, Resolution.Daily, TickType.Quote, TimeSpan.FromDays(365));

                // unsupported tick type
                // yield return new TestCaseData(AAPL, Resolution.Tick, TickType.OpenInterest, TimeSpan.FromMinutes(5));
            }
        }


        [Test, TestCaseSource(nameof(TestParameters))]
        public void GetHistoricalData(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
        {
            var historyRequests = new List<HistoryRequest> { CreateHistoryRequest(symbol, resolution, tickType, period) };

            var historyResponse = _historyProvider.GetHistory(historyRequests, TimeZones.Utc).SelectMany(x => x.AllData).ToList();

            Assert.IsNotEmpty(historyResponse);

            if (resolution > Resolution.Tick)
            {
                // No repeating bars
                var timesArray = historyResponse.Select(x => x.Time).ToList();
                Assert.That(timesArray.Distinct().Count(), Is.EqualTo(timesArray.Count));

                // Resolution is respected
                var timeSpan = resolution.ToTimeSpan();
                Assert.That(historyResponse, Is.All.Matches<BaseData>(x => x.EndTime - x.Time == timeSpan),
                    $"All bars periods should be equal to {timeSpan} ({resolution})");
            }
            else
            {
                // All data in the slice are ticks
                Assert.That(historyResponse, Is.All.Matches<BaseData>(tick => tick.GetType() == typeof(Tick)));
            }
        }

        internal static HistoryRequest CreateHistoryRequest(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
        {
            var end = new DateTime(2024, 01, 17, 16, 30, 0);

            if (resolution == Resolution.Daily)
            {
                end = end.Date.AddDays(1);
            }
            var dataType = LeanData.GetDataType(resolution, tickType);

            return new HistoryRequest(end.Subtract(period),
                end,
                dataType,
                symbol,
                resolution,
                SecurityExchangeHours.AlwaysOpen(TimeZones.NewYork),
                TimeZones.NewYork,
                null,
                true,
                false,
                DataNormalizationMode.Adjusted,
                tickType);
        }
    }
}
