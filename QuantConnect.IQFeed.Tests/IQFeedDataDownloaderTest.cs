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
using QuantConnect.Securities;
using System.Collections.Generic;
using QuantConnect.Configuration;

namespace QuantConnect.Lean.DataSource.IQFeed.Tests
{
    [TestFixture, Explicit("This tests require a IQFeed credentials.")]
    public class IQFeedDataDownloaderTest
    {
        private IQFeedDataDownloader _downloader;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            if (OS.IsWindows)
            {
                // IQConnect is only supported on Windows
                var connector = new IQConnect(Config.Get("iqfeed-productName"), "1.0");
                Assert.IsTrue(connector.Launch(), "Failed to launch IQConnect on Windows. Ensure IQFeed is installed and configured properly.");
            }

            _downloader = new IQFeedDataDownloader();
        }

        private static IEnumerable<TestCaseData> HistoricalDataTestCases => IQFeedHistoryProviderTests.HistoricalTestParameters;

        [TestCaseSource(nameof(HistoricalDataTestCases))]
        public void DownloadsHistoricalData(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period, bool isNullResult)
        {
            var request = IQFeedHistoryProviderTests.CreateHistoryRequest(symbol, resolution, tickType, period);

            var parameters = new DataDownloaderGetParameters(symbol, resolution, request.StartTimeUtc, request.EndTimeUtc, tickType);
            var downloadResponse = _downloader.Get(parameters)?.ToList();

            if (isNullResult)
            {
                Assert.IsNull(downloadResponse);
                return;
            }

            IQFeedHistoryProviderTests.AssertHistoricalDataResponse(resolution, downloadResponse);
        }

        private static IEnumerable<TestCaseData> CanonicalFutureSymbolTestCases
        {
            get
            {
                var startDateUtc = new DateTime(2025, 03, 03);
                var endDateUtc = new DateTime(2025, 04, 04);

                var naturalGas = Symbol.Create(Futures.Energy.NaturalGas, SecurityType.Future, Market.NYMEX);
                yield return new TestCaseData(naturalGas, Resolution.Daily, TickType.Trade, startDateUtc, endDateUtc);

                var nasdaq100EMini = Symbol.Create(Futures.Indices.NASDAQ100EMini, SecurityType.Future, Market.CME);
                yield return new TestCaseData(nasdaq100EMini, Resolution.Daily, TickType.Trade, startDateUtc, endDateUtc);
            }
        }


        [TestCaseSource(nameof(CanonicalFutureSymbolTestCases))]
        public void DownloadCanonicalFutureHistoricalData(Symbol symbol, Resolution resolution, TickType tickType, DateTime startDateUtc, DateTime endDateUtc)
        {
            var request = IQFeedHistoryProviderTests.CreateHistoryRequest(symbol, resolution, tickType, startDateUtc, endDateUtc);

            var parameters = new DataDownloaderGetParameters(symbol, resolution, request.StartTimeUtc, request.EndTimeUtc, tickType);
            var downloadResponse = _downloader.Get(parameters)?.ToList();

            Assert.IsNotNull(downloadResponse);
            Assert.IsNotEmpty(downloadResponse);
        }
    }
}
