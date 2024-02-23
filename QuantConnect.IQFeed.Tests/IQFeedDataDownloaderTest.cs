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
using System.Collections.Generic;

namespace QuantConnect.Lean.DataSource.IQFeed.Tests
{
    [TestFixture, Explicit("This tests require a IQFeed credentials.")]
    public class IQFeedDataDownloaderTest
    {
        private IQFeedDataDownloader _downloader;

        [SetUp]
        public void SetUp()
        {
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
    }
}
