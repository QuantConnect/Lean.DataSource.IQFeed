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

using QuantConnect.Data;
using QuantConnect.Securities;
using QuantConnect.Data.Market;
using IQFeed.CSharpApiClient.Lookup;
using QuantConnect.Configuration;

namespace QuantConnect.IQFeed.Downloader
{
    /// <summary>
    /// Represents a data downloader for retrieving historical market data using IQFeed.
    /// </summary>
    public class IQFeedDataDownloader : IDataDownloader
    {
        /// <summary>
        /// The number of IQFeed clients to use for parallel processing.
        /// </summary>
        private const int NumberOfClients = 8;

        /// <summary>
        /// The file history provider used by the data downloader.
        /// </summary>
        private readonly IQFeedFileHistoryProvider _fileHistoryProvider;

        /// <summary>
        /// Initializes a new instance of the <see cref="IQFeedDataDownloader"/> class.
        /// </summary>
        public IQFeedDataDownloader()
        {
            // Create and connect the IQFeed lookup client
            var lookupClient = LookupClientFactory.CreateNew(Config.Get("iqfeed-host", "127.0.0.1"), 9100, NumberOfClients);
            // Establish connection with IQFeed Client
            lookupClient.Connect();

            _fileHistoryProvider = new IQFeedFileHistoryProvider(lookupClient, new IQFeedDataQueueUniverseProvider(), MarketHoursDatabase.FromDataFolder());
        }

        /// <summary>
        /// Get historical data enumerable for a single symbol, type and resolution given this start and end time (in UTC).
        /// </summary>
        /// <param name="dataDownloaderGetParameters">model class for passing in parameters for historical data</param>
        /// <returns>Enumerable of base data for this symbol</returns>
        public IEnumerable<BaseData> Get(DataDownloaderGetParameters dataDownloaderGetParameters)
        {
            var symbol = dataDownloaderGetParameters.Symbol;
            var resolution = dataDownloaderGetParameters.Resolution;
            var startUtc = dataDownloaderGetParameters.StartUtc;
            var endUtc = dataDownloaderGetParameters.EndUtc;
            var tickType = dataDownloaderGetParameters.TickType;

            if (tickType == TickType.OpenInterest)
            {
                return Enumerable.Empty<BaseData>();
            }

            if (symbol.ID.SecurityType != SecurityType.Equity)
                throw new NotSupportedException("SecurityType not available: " + symbol.ID.SecurityType);

            if (endUtc < startUtc)
                throw new ArgumentException("The end date must be greater or equal than the start date.");

            var dataType = resolution == Resolution.Tick ? typeof(Tick) : typeof(TradeBar);

            return _fileHistoryProvider.ProcessHistoryRequests(
                new HistoryRequest(
                    startUtc,
                    endUtc,
                    dataType,
                    symbol,
                    resolution,
                    SecurityExchangeHours.AlwaysOpen(TimeZones.NewYork),
                    TimeZones.NewYork,
                    resolution,
                    true,
                    false,
                    DataNormalizationMode.Adjusted,
                    tickType));
        }
    }
}
