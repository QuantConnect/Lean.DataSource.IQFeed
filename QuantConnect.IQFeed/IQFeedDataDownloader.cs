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
            _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
            _dataQueueUniverseProvider = new IQFeedDataQueueUniverseProvider();

            _fileHistoryProviderLazy = new Lazy<IQFeedFileHistoryProvider>(() =>
            {
                // Create and connect the IQFeed lookup client
                var lookupClient = LookupClientFactory.CreateNew(Config.Get("iqfeed-host", "127.0.0.1"), IQSocket.GetPort(PortType.Lookup), NumberOfClients, LookupDefault.Timeout);
                // Establish connection with IQFeed Client
                lookupClient.Connect();

                return new IQFeedFileHistoryProvider(lookupClient, _dataQueueUniverseProvider, MarketHoursDatabase.FromDataFolder());
            });
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

            if (symbol.IsCanonical())
            {
                return GetCanonicalSymbolHistory(
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
        /// Retrieves historical data for all individual tradeable contracts derived from a given canonical symbol
        /// (such as options, futures, or other security types) within the specified date range, resolution, and tick type.
        /// </summary>
        /// <param name="symbol">The canonical <see cref="Symbol"/> representing the underlying security group (option chain, future chain, etc.).</param>
        /// <param name="startUtc">The UTC start time of the historical data request.</param>
        /// <param name="endUtc">The UTC end time of the historical data request.</param>
        /// <param name="dataType">The type of data to retrieve, such as <see cref="TradeBar"/>, <see cref="QuoteBar"/>, or <see cref="Tick"/>.</param>
        /// <param name="resolution">The resolution of the historical data (e.g., Minute, Hour, Daily).</param>
        /// <param name="exchangeHours">The exchange hours for the underlying security to correctly filter trading times.</param>
        /// <param name="dataTimeZone">The time zone for the timestamps in the returned data.</param>
        /// <param name="tickType">The tick type to retrieve, such as Trade or Quote ticks.</param>
        /// <returns>
        /// An enumerable collection of <see cref="BaseData"/> instances representing historical data for all tradeable contracts
        /// derived from the canonical symbol within the requested date range. Returns <c>null</c> if no data was found.
        /// </returns>
        private IEnumerable<BaseData>? GetCanonicalSymbolHistory(Symbol symbol, DateTime startUtc, DateTime endUtc, Type dataType,
            Resolution resolution, SecurityExchangeHours exchangeHours, DateTimeZone dataTimeZone, TickType tickType)
        {
            var blockingCollection = new BlockingCollection<BaseData>();
            var symbols = GetCanonicalSymbolChain(symbol, startUtc, endUtc);

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
                    blockingCollection.Add(data);
                }
            })).ContinueWith(_ =>
            {
                blockingCollection.CompleteAdding();
            });

            var options = blockingCollection.GetConsumingEnumerable();

            // Validate if the collection contains at least one successful response from history.
            if (!options.Any())
            {
                return null;
            }

            return options;
        }

        /// <summary>
        /// Retrieves a distinct set of tradeable option symbols for the specified underlying security
        /// within the given date range, filtered to trading days as defined by the market hours database.
        /// This includes all option types such as equity options, futures options, index options, and more,
        /// that are available for trading on those dates.
        /// </summary>
        /// <param name="symbol">The canonical symbol representing the underlying security (e.g., equity, future, or index).</param>
        /// <param name="startUtc">The UTC start date of the lookup period.</param>
        /// <param name="endUtc">The UTC end date of the lookup period.</param>
        /// <returns>
        /// An enumerable collection of unique <see cref="Symbol"/> instances representing tradeable options
        /// for the specified underlying symbol over the specified date range.
        /// </returns>
        protected virtual IEnumerable<Symbol> GetCanonicalSymbolChain(Symbol symbol, DateTime startUtc, DateTime endUtc)
        {
            var exchangeHours = _marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);

            return QuantConnect.Time.EachTradeableDay(exchangeHours, startUtc.Date, endUtc.Date)
                .Select(date => _dataQueueUniverseProvider.LookupSymbols(symbol, default, default))
                .SelectMany(x => x)
                .Distinct();
        }
    }
}
