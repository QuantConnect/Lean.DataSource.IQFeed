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

using NodaTime;
using QuantConnect.Data;
using QuantConnect.Securities;
using QuantConnect.Data.Market;
using QuantConnect.Configuration;
using IQFeed.CSharpApiClient.Lookup;
using System.Collections.Concurrent;

namespace QuantConnect.Lean.DataSource.IQFeed
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
        /// Provides access to all available symbols corresponding to a canonical symbol using the IQFeed data source.
        /// </summary>
        private readonly IQFeedDataQueueUniverseProvider _dataQueueUniverseProvider;

        /// <summary>
        /// Provides access to exchange hours and raw data times zones in various markets
        /// </summary>
        private readonly MarketHoursDatabase _marketHoursDatabase;

        /// <summary>
        /// The file history provider used by the data downloader.
        /// </summary>
        protected readonly IQFeedFileHistoryProvider _fileHistoryProvider;

        /// <summary>
        /// Initializes a new instance of the <see cref="IQFeedDataDownloader"/> class.
        /// </summary>
        public IQFeedDataDownloader()
        {
            _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();

            _dataQueueUniverseProvider = new IQFeedDataQueueUniverseProvider();

            var lookupClient = LookupClientFactory.CreateNew(Config.Get("iqfeed-host", "127.0.0.1"), IQSocket.GetPort(PortType.Lookup), NumberOfClients, LookupDefault.Timeout);

            lookupClient.Connect();

            _fileHistoryProvider = new IQFeedFileHistoryProvider(lookupClient, _dataQueueUniverseProvider, MarketHoursDatabase.FromDataFolder());
        }

        /// <summary>
        /// Get historical data enumerable for a single symbol, type and resolution given this start and end time (in UTC).
        /// </summary>
        /// <param name="dataDownloaderGetParameters">model class for passing in parameters for historical data</param>
        /// <returns>Enumerable of base data for this symbol</returns>
        public IEnumerable<BaseData>? Get(DataDownloaderGetParameters dataDownloaderGetParameters)
        {
            var symbol = dataDownloaderGetParameters.Symbol;
            var resolution = dataDownloaderGetParameters.Resolution;
            var startUtc = dataDownloaderGetParameters.StartUtc;
            var endUtc = dataDownloaderGetParameters.EndUtc;
            var tickType = dataDownloaderGetParameters.TickType;

            var exchangeHours = _marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);
            var dataTimeZone = _marketHoursDatabase.GetDataTimeZone(symbol.ID.Market, symbol, symbol.SecurityType);

            var dataType = resolution == Resolution.Tick ? typeof(Tick) : typeof(TradeBar);

            Logging.Log.Trace($"IS Symbol Cannonical? {symbol} = {symbol.IsCanonical()}");

            if (symbol.IsCanonical())
            {
                return GetCanonicalOptionHistory(
                    symbol,
                    startUtc,
                    endUtc,
                    dataType,
                    resolution,
                    exchangeHours,
                    dataTimeZone,
                    dataDownloaderGetParameters.TickType);
            }
            else
            {
                return _fileHistoryProvider.ProcessHistoryRequests(
                    new HistoryRequest(
                        startUtc,
                        endUtc,
                        dataType,
                        symbol,
                        resolution,
                        exchangeHours,
                        dataTimeZone,
                        resolution,
                        true,
                        false,
                        DataNormalizationMode.Adjusted,
                        tickType));
            }
        }

        /// <summary>
        /// Retrieves historical data for all available option contracts derived from a given canonical option symbol
        /// within the specified date range and resolution.
        /// </summary>
        /// <param name="symbol">The canonical option <see cref="Symbol"/> used to derive individual contracts.</param>
        /// <param name="startUtc">The UTC start time for the historical data request.</param>
        /// <param name="endUtc">The UTC end time for the historical data request.</param>
        /// <param name="dataType">The type of data to retrieve (e.g., <see cref="TradeBar"/>, <see cref="QuoteBar"/>, <see cref="Tick"/>).</param>
        /// <param name="resolution">The resolution of the historical data (e.g., Minute, Hour, Daily).</param>
        /// <param name="exchangeHours">The exchange hours of the underlying security.</param>
        /// <param name="dataTimeZone">The time zone in which the data timestamps are represented.</param>
        /// <param name="tickType">The tick type of data to retrieve (e.g., Trade, Quote).</param>
        /// <returns>
        /// A collection of <see cref="BaseData"/> representing historical option data, or <c>null</c> if no data was found.
        /// </returns>
        private IEnumerable<BaseData>? GetCanonicalOptionHistory(Symbol symbol, DateTime startUtc, DateTime endUtc, Type dataType,
            Resolution resolution, SecurityExchangeHours exchangeHours, DateTimeZone dataTimeZone, TickType tickType)
        {
            var blockingOptionCollection = new BlockingCollection<BaseData>();
            var symbols = GetOptions(symbol, startUtc, endUtc);

            // Symbol can have a lot of Option parameters
            Task.Run(() => Parallel.ForEach(symbols, targetSymbol =>
            {
                var historyRequest = new HistoryRequest(startUtc, endUtc, dataType, targetSymbol, resolution, exchangeHours, dataTimeZone,
                    resolution, true, false, DataNormalizationMode.Raw, tickType);

                var history = _fileHistoryProvider.ProcessHistoryRequests(historyRequest);

                // If history is null, it indicates an incorrect or missing request for historical data,
                // so we skip processing for this symbol and move to the next one.
                if (history == null)
                {
                    return;
                }

                foreach (var data in history)
                {
                    blockingOptionCollection.Add(data);
                }
            })).ContinueWith(_ =>
            {
                blockingOptionCollection.CompleteAdding();
            });

            var options = blockingOptionCollection.GetConsumingEnumerable();

            // Validate if the collection contains at least one successful response from history.
            if (!options.Any())
            {
                return null;
            }

            return options;
        }

        /// <summary>
        /// Retrieves a distinct set of option symbols for the specified underlying symbol
        /// within the given date range, limited to tradeable days as defined by the market hours database.
        /// </summary>
        /// <param name="symbol">The canonical symbol representing the underlying security.</param>
        /// <param name="startUtc">The UTC start date of the lookup period.</param>
        /// <param name="endUtc">The UTC end date of the lookup period.</param>
        /// <returns>
        /// An enumerable collection of unique option <see cref="Symbol"/> instances
        /// that match the specified underlying symbol over the specified date range.
        /// </returns>
        protected virtual IEnumerable<Symbol> GetOptions(Symbol symbol, DateTime startUtc, DateTime endUtc)
        {
            var exchangeHours = _marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);

            return QuantConnect.Time.EachTradeableDay(exchangeHours, startUtc.Date, endUtc.Date)
                .Select(date => _dataQueueUniverseProvider.LookupSymbols(symbol, default, default))
                .SelectMany(x => x)
                .Distinct();
        }
    }
}
