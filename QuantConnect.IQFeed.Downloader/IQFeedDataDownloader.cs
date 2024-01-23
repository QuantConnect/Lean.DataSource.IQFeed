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
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Data.Market;
using QuantConnect.Configuration;
using IQFeed.CSharpApiClient.Lookup;

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
        /// Lazy initialization for the IQFeed file history provider.
        /// </summary>
        /// <remarks>
        /// This lazy initialization is used to provide deferred creation of the <see cref="IQFeedFileHistoryProvider"/>.
        /// </remarks>
        private Lazy<IQFeedFileHistoryProvider> _fileHistoryProviderLazy;

        /// <summary>
        /// The file history provider used by the data downloader.
        /// </summary>
        protected IQFeedFileHistoryProvider _fileHistoryProvider => _fileHistoryProviderLazy.Value;

        /// <summary>
        /// Initializes a new instance of the <see cref="IQFeedDataDownloader"/> class.
        /// </summary>
        public IQFeedDataDownloader()
        {
            _fileHistoryProviderLazy = new Lazy<IQFeedFileHistoryProvider>(() =>
            {                
                // Create and connect the IQFeed lookup client
                var lookupClient = LookupClientFactory.CreateNew(Config.Get("iqfeed-host", "127.0.0.1"), IQSocket.GetPort(PortType.Lookup), NumberOfClients);
                // Establish connection with IQFeed Client
                lookupClient.Connect();

                return new IQFeedFileHistoryProvider(lookupClient, new IQFeedDataQueueUniverseProvider(), MarketHoursDatabase.FromDataFolder());
            });
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
            {
                return Enumerable.Empty<BaseData>();
            }

            if (tickType == TickType.Quote && resolution != Resolution.Tick)
            {
                Log.Trace($"{nameof(IQFeedDataDownloader)}.{nameof(Get)}: Historical data request with TickType 'Quote' is not supported for resolutions other than Tick. Requested Resolution: {resolution}");
                return Enumerable.Empty<BaseData>();
            }

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
