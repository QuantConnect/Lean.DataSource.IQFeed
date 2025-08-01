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
using RestSharp;
using System.Net;
using System.Text;
using Newtonsoft.Json;
using QuantConnect.Api;
using QuantConnect.Util;
using QuantConnect.Data;
using Newtonsoft.Json.Linq;
using QuantConnect.Packets;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Interfaces;
using QuantConnect.Data.Market;
using QuantConnect.Configuration;
using System.Security.Cryptography;
using System.Collections.Concurrent;
using System.Net.NetworkInformation;
using HistoryRequest = QuantConnect.Data.HistoryRequest;

namespace QuantConnect.Lean.DataSource.IQFeed
{
    /// <summary>
    /// IQFeedDataProvider is an implementation of IDataQueueHandler and IHistoryProvider
    /// </summary>
    public class IQFeedDataProvider : HistoryProviderBase, IDataQueueHandler, IDataQueueUniverseProvider
    {
        private bool _isConnected;
        private readonly HashSet<Symbol> _symbols;
        private readonly Dictionary<Symbol, Symbol> _underlyings;
        private readonly object _sync = new object();
        private IQFeedDataQueueUniverseProvider _symbolUniverse;

        /// <summary>
        /// Represents the time zone used by IQFeed, which returns time in the New York (EST) Time Zone with daylight savings time.
        /// </summary>
        public readonly static DateTimeZone TimeZoneIQFeed = TimeZones.NewYork;

        //Socket connections:
        private AdminPort _adminPort;
        private Level1Port _level1Port;
        private HistoryPort _historyPort;

        private readonly IDataAggregator _aggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(
            Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager"), forceTypeNameOnExisting: false);
        private readonly EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager;

        /// <summary>
        /// Gets the total number of data points emitted by this history provider
        /// </summary>
        public override int DataPointCount { get; } = 0;

        /// <summary>
        /// IQFeedDataProvider is an implementation of IDataQueueHandler:
        /// </summary>
        public IQFeedDataProvider()
        {
            _symbols = new HashSet<Symbol>();
            _underlyings = new Dictionary<Symbol, Symbol>();
            _subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();
            _subscriptionManager.SubscribeImpl += (s, t) =>
            {
                Subscribe(s);
                return true;
            };

            _subscriptionManager.UnsubscribeImpl += (s, t) =>
            {
                Unsubscribe(s);
                return true;
            };

            ValidateSubscription();

            if (!IsConnected) Connect();
        }

        /// <summary>
        /// Subscribe to the specified configuration
        /// </summary>
        /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
        /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
        /// <returns>The new enumerator for this subscription request</returns>
        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            if (!CanSubscribe(dataConfig.Symbol))
            {
                return null;
            }

            var enumerator = _aggregator.Add(dataConfig, newDataAvailableHandler);
            _subscriptionManager.Subscribe(dataConfig);

            return enumerator;
        }

        /// <summary>
        /// Adds the specified symbols to the subscription: new IQLevel1WatchItem("IBM", true)
        /// </summary>
        /// <param name="symbols">The symbols to be added keyed by SecurityType</param>
        public void Subscribe(IEnumerable<Symbol> symbols)
        {
            try
            {
                foreach (var symbol in symbols)
                {
                    lock (_sync)
                    {
                        Log.Trace("IQFeed.Subscribe(): Subscribe Request: " + symbol.ToString());

                        if (_symbols.Add(symbol))
                        {
                            // processing canonical option symbol to subscribe to underlying prices
                            var subscribeSymbol = symbol;

                            if (symbol.ID.SecurityType == SecurityType.Option && symbol.IsCanonical())
                            {
                                subscribeSymbol = symbol.Underlying;
                                _underlyings.Add(subscribeSymbol, symbol);
                            }

                            if (symbol.ID.SecurityType == SecurityType.Future && symbol.IsCanonical())
                            {
                                // do nothing for now. Later might add continuous contract symbol.
                                return;
                            }

                            var ticker = _symbolUniverse.GetBrokerageSymbol(subscribeSymbol);

                            if (!string.IsNullOrEmpty(ticker))
                            {
                                _level1Port.Subscribe(ticker);
                                Log.Trace("IQFeed.Subscribe(): Subscribe Processed: {0} ({1})", symbol.Value, ticker);
                            }
                            else
                            {
                                Log.Error("IQFeed.Subscribe(): Symbol {0} was not found in IQFeed symbol universe", symbol.Value);
                            }
                        }
                    }
                }
            }
            catch (Exception err)
            {
                Log.Error("IQFeed.Subscribe(): " + err.Message);
            }
        }

        /// <summary>
        /// Removes the specified configuration
        /// </summary>
        /// <param name="dataConfig">Subscription config to be removed</param>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            _subscriptionManager.Unsubscribe(dataConfig);
            _aggregator.Remove(dataConfig);
        }

        /// <summary>
        /// Sets the job we're subscribing for
        /// </summary>
        /// <param name="job">Job we're subscribing for</param>
        public void SetJob(LiveNodePacket job)
        {
        }

        /// <summary>
        /// Removes the specified symbols to the subscription
        /// </summary>
        /// <param name="symbols">The symbols to be removed keyed by SecurityType</param>
        public void Unsubscribe(IEnumerable<Symbol> symbols)
        {
            try
            {
                foreach (var symbol in symbols)
                {
                    lock (_sync)
                    {
                        Log.Trace("IQFeed.Unsubscribe(): " + symbol.ToString());

                        _symbols.Remove(symbol);

                        var subscribeSymbol = symbol;

                        if (symbol.ID.SecurityType == SecurityType.Option && symbol.ID.StrikePrice == 0.0m)
                        {
                            subscribeSymbol = symbol.Underlying;
                            _underlyings.Remove(subscribeSymbol);
                        }

                        var ticker = _symbolUniverse.GetBrokerageSymbol(subscribeSymbol);
                        if (_level1Port.Contains(ticker))
                        {
                            _level1Port.Unsubscribe(ticker);
                        }
                    }
                }
            }
            catch (Exception err)
            {
                Log.Error("IQFeed.Unsubscribe(): " + err.Message);
            }
        }

        /// <summary>
        /// Initializes this history provider to work for the specified job
        /// </summary>
        /// <param name="parameters">The initialization parameters</param>
        public override void Initialize(HistoryProviderInitializeParameters parameters)
        {
        }

        /// <summary>
        /// Gets the history for the requested securities
        /// </summary>
        /// <param name="requests">The historical data requests</param>
        /// <param name="sliceTimeZone">The time zone used when time stamping the slice instances</param>
        /// <returns>An enumerable of the slices of data covering the span specified in each request</returns>
        public override IEnumerable<Slice>? GetHistory(IEnumerable<HistoryRequest> requests, DateTimeZone sliceTimeZone)
        {
            var subscriptions = new List<IEnumerable<Slice>>();
            foreach (var request in requests)
            {
                var history = _historyPort.ProcessHistoryRequests(request, sliceTimeZone);

                if (history == null)
                {
                    continue;
                }

                subscriptions.Add(history);
            }

            if (subscriptions.Count == 0)
            {
                return null;
            }

            return subscriptions.SelectMany(x => x);
        }

        /// <summary>
        /// Indicates the connection is live.
        /// </summary>
        public bool IsConnected => _isConnected;

        /// <summary>
        /// Connect to the IQ Feed using supplied username and password information.
        /// </summary>
        private void Connect()
        {
            try
            {
                // Launch the IQ Feed Application:
                Log.Trace("IQFeed.Connect(): Launching client...");

                if (OS.IsWindows)
                {
                    // IQConnect is only supported on Windows
                    var connector = new IQConnect(Config.Get("iqfeed-productName"), "1.0");
                    connector.Launch();
                }

                // Initialise one admin port
                Log.Trace("IQFeed.Connect(): Connecting to admin...");
                _adminPort = new AdminPort();
                _adminPort.Connect();
                _adminPort.SetAutoconnect();
                _adminPort.SetClientStats(false);
                _adminPort.SetClientName("Admin");

                _adminPort.DisconnectedEvent += AdminPortOnDisconnectedEvent;
                _adminPort.ConnectedEvent += AdminPortOnConnectedEvent;

                _symbolUniverse = new IQFeedDataQueueUniverseProvider();

                Log.Trace("IQFeed.Connect(): Connecting to L1 data...");
                _level1Port = new Level1Port(_aggregator, _symbolUniverse);
                _level1Port.Connect();
                _level1Port.SetClientName("Level1");

                Log.Trace("IQFeed.Connect(): Connecting to Historical data...");
                _historyPort = new HistoryPort(_symbolUniverse);
                _historyPort.Connect();
                _historyPort.SetClientName("History");

                _isConnected = true;
            }
            catch (Exception err)
            {
                Log.Error("IQFeed.Connect(): Error Connecting to IQFeed: " + err.Message);
                _isConnected = false;
            }
        }

        /// <summary>
        /// Disconnect from all ports we're subscribed to:
        /// </summary>
        /// <remarks>
        /// Not being used. IQ automatically disconnect on killing LEAN
        /// </remarks>
        private void Disconnect()
        {
            if (_adminPort != null) _adminPort.Disconnect();
            if (_level1Port != null) _level1Port.Disconnect();
            _isConnected = false;
            Log.Trace("IQFeed.Disconnect(): Disconnected");
        }


        /// <summary>
        /// Returns true if this data provide can handle the specified symbol
        /// </summary>
        /// <param name="symbol">The symbol to be handled</param>
        /// <returns>True if this data provider can get data for the symbol, false otherwise</returns>
        private static bool CanSubscribe(Symbol symbol)
        {
            var market = symbol.ID.Market;
            var securityType = symbol.ID.SecurityType;

            if (symbol.Value.IndexOfInvariant("universe", true) != -1) return false;

            return
                (securityType == SecurityType.Equity && market == Market.USA) ||
                (securityType == SecurityType.Forex && market == Market.FXCM) ||
                (securityType == SecurityType.Option && market == Market.USA) ||
                (securityType == SecurityType.Future);
        }

        /// <summary>
        /// Admin port is connected.
        /// </summary>
        private void AdminPortOnConnectedEvent(object sender, ConnectedEventArgs connectedEventArgs)
        {
            _isConnected = true;
            Log.Error("IQFeed.AdminPortOnConnectedEvent(): ADMIN PORT CONNECTED!");
        }

        /// <summary>
        /// Admin port disconnected from the IQFeed server.
        /// </summary>
        private void AdminPortOnDisconnectedEvent(object sender, DisconnectedEventArgs disconnectedEventArgs)
        {
            _isConnected = false;
            Log.Error("IQFeed.AdminPortOnDisconnectedEvent(): ADMIN PORT DISCONNECTED!");
        }

        /// <summary>
        /// Method returns a collection of Symbols that are available at the data source.
        /// </summary>
        /// <param name="lookupName">String representing the name to lookup</param>
        /// <param name="securityType">Expected security type of the returned symbols (if any)</param>
        /// <param name="includeExpired">Include expired contracts</param>
        /// <param name="securityCurrency">Expected security currency(if any)</param>
        /// <param name="securityExchange">Expected security exchange name(if any)</param>
        /// <returns>Symbol results</returns>
        public IEnumerable<Symbol> LookupSymbols(string lookupName, SecurityType securityType, bool includeExpired, string securityCurrency = null, string securityExchange = null)
        {
            return _symbolUniverse.LookupSymbols(lookupName, securityType, includeExpired, securityCurrency, securityExchange);
        }

        /// <summary>
        /// Method returns a collection of Symbols that are available at the data source.
        /// </summary>
        /// <param name="symbol">Symbol to lookup</param>
        /// <param name="includeExpired">Include expired contracts</param>
        /// <param name="securityCurrency">Expected security currency(if any)</param>
        /// <returns>Symbol results</returns>
        public IEnumerable<Symbol> LookupSymbols(Symbol symbol, bool includeExpired, string securityCurrency)
        {
            return LookupSymbols(symbol.ID.Symbol, symbol.SecurityType, includeExpired, securityCurrency);
        }

        /// <summary>
        /// Returns whether selection can take place or not.
        /// </summary>
        /// <returns>True if selection can take place</returns>
        public bool CanPerformSelection()
        {
            return _symbolUniverse.CanPerformSelection();
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            _symbolUniverse.DisposeSafely();
        }

        private class ModulesReadLicenseRead : Api.RestResponse
        {
            [JsonProperty(PropertyName = "license")]
            public string License;

            [JsonProperty(PropertyName = "organizationId")]
            public string OrganizationId;
        }

        /// <summary>
        /// Validate the user of this project has permission to be using it via our web API.
        /// </summary>
        private static void ValidateSubscription()
        {
            try
            {
                const int productId = 332;
                var userId = Globals.UserId;
                var token = Globals.UserToken;
                var organizationId = Globals.OrganizationID;
                // Verify we can authenticate with this user and token
                var api = new ApiConnection(userId, token);
                if (!api.Connected)
                {
                    throw new ArgumentException("Invalid api user id or token, cannot authenticate subscription.");
                }
                // Compile the information we want to send when validating
                var information = new Dictionary<string, object>()
                {
                    {"productId", productId},
                    {"machineName", Environment.MachineName},
                    {"userName", Environment.UserName},
                    {"domainName", Environment.UserDomainName},
                    {"os", Environment.OSVersion}
                };
                // IP and Mac Address Information
                try
                {
                    var interfaceDictionary = new List<Dictionary<string, object>>();
                    foreach (var nic in NetworkInterface.GetAllNetworkInterfaces().Where(nic => nic.OperationalStatus == OperationalStatus.Up))
                    {
                        var interfaceInformation = new Dictionary<string, object>();
                        // Get UnicastAddresses
                        var addresses = nic.GetIPProperties().UnicastAddresses
                            .Select(uniAddress => uniAddress.Address)
                            .Where(address => !IPAddress.IsLoopback(address)).Select(x => x.ToString());
                        // If this interface has non-loopback addresses, we will include it
                        if (!addresses.IsNullOrEmpty())
                        {
                            interfaceInformation.Add("unicastAddresses", addresses);
                            // Get MAC address
                            interfaceInformation.Add("MAC", nic.GetPhysicalAddress().ToString());
                            // Add Interface name
                            interfaceInformation.Add("name", nic.Name);
                            // Add these to our dictionary
                            interfaceDictionary.Add(interfaceInformation);
                        }
                    }
                    information.Add("networkInterfaces", interfaceDictionary);
                }
                catch (Exception)
                {
                    // NOP, not necessary to crash if fails to extract and add this information
                }
                // Include our OrganizationId if specified
                if (!string.IsNullOrEmpty(organizationId))
                {
                    information.Add("organizationId", organizationId);
                }
                var request = new RestRequest("modules/license/read", Method.POST) { RequestFormat = DataFormat.Json };
                request.AddParameter("application/json", JsonConvert.SerializeObject(information), ParameterType.RequestBody);
                api.TryRequest(request, out ModulesReadLicenseRead result);
                if (!result.Success)
                {
                    throw new InvalidOperationException($"Request for subscriptions from web failed, Response Errors : {string.Join(',', result.Errors)}");
                }

                var encryptedData = result.License;
                // Decrypt the data we received
                DateTime? expirationDate = null;
                long? stamp = null;
                bool? isValid = null;
                if (encryptedData != null)
                {
                    // Fetch the org id from the response if it was not set, we need it to generate our validation key
                    if (string.IsNullOrEmpty(organizationId))
                    {
                        organizationId = result.OrganizationId;
                    }
                    // Create our combination key
                    var password = $"{token}-{organizationId}";
                    var key = SHA256.HashData(Encoding.UTF8.GetBytes(password));
                    // Split the data
                    var info = encryptedData.Split("::");
                    var buffer = Convert.FromBase64String(info[0]);
                    var iv = Convert.FromBase64String(info[1]);
                    // Decrypt our information
                    using var aes = new AesManaged();
                    var decryptor = aes.CreateDecryptor(key, iv);
                    using var memoryStream = new MemoryStream(buffer);
                    using var cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read);
                    using var streamReader = new StreamReader(cryptoStream);
                    var decryptedData = streamReader.ReadToEnd();
                    if (!decryptedData.IsNullOrEmpty())
                    {
                        var jsonInfo = JsonConvert.DeserializeObject<JObject>(decryptedData);
                        expirationDate = jsonInfo["expiration"]?.Value<DateTime>();
                        isValid = jsonInfo["isValid"]?.Value<bool>();
                        stamp = jsonInfo["stamped"]?.Value<int>();
                    }
                }
                // Validate our conditions
                if (!expirationDate.HasValue || !isValid.HasValue || !stamp.HasValue)
                {
                    throw new InvalidOperationException("Failed to validate subscription.");
                }

                var nowUtc = DateTime.UtcNow;
                var timeSpan = nowUtc - QuantConnect.Time.UnixTimeStampToDateTime(stamp.Value);
                if (timeSpan > TimeSpan.FromHours(12))
                {
                    throw new InvalidOperationException("Invalid API response.");
                }
                if (!isValid.Value)
                {
                    throw new ArgumentException($"Your subscription is not valid, please check your product subscriptions on our website.");
                }
                if (expirationDate < nowUtc)
                {
                    throw new ArgumentException($"Your subscription expired {expirationDate}, please renew in order to use this product.");
                }
            }
            catch (Exception e)
            {
                Log.Error($"PolygonDataQueueHandler.ValidateSubscription(): Failed during validation, shutting down. Error : {e.Message}");
                throw;
            }
        }
    }

    /// <summary>
    /// Admin class type
    /// </summary>
    public class AdminPort : IQAdminSocketClient
    {
        public AdminPort()
            : base(IQFeedDefault.BufferSize)
        {
        }
    }

    /// <summary>
    /// Level 1 Data Request:
    /// </summary>
    public class Level1Port : IQLevel1Client
    {
        private readonly ConcurrentDictionary<string, double> _prices;
        private readonly ConcurrentDictionary<string, int> _openInterests;
        private readonly IQFeedDataQueueUniverseProvider _symbolUniverse;
        private readonly IDataAggregator _aggregator;

        /// <summary>
        /// A thread-safe dictionary that maps a <see cref="Symbol"/> to a <see cref="DateTimeZone"/>.
        /// </summary>
        /// <remarks>
        /// This dictionary is used to store the time zone information for each symbol in a concurrent environment,
        /// ensuring thread safety when accessing or modifying the time zone data.
        /// </remarks>
        private readonly ConcurrentDictionary<Symbol, DateTimeZone> _exchangeTimeZoneByLeanSymbol = new();

        public Level1Port(IDataAggregator aggregator, IQFeedDataQueueUniverseProvider symbolUniverse)
            : base(IQFeedDefault.BufferSize)
        {
            _prices = new ConcurrentDictionary<string, double>();
            _openInterests = new ConcurrentDictionary<string, int>();

            _aggregator = aggregator;
            _symbolUniverse = symbolUniverse;
            Level1SummaryUpdateEvent += OnLevel1SummaryUpdateEvent;
            Level1ServerDisconnectedEvent += OnLevel1ServerDisconnected;
            Level1ServerReconnectFailed += OnLevel1ServerReconnectFailed;
            Level1UnknownEvent += OnLevel1UnknownEvent;
            Level1FundamentalEvent += OnLevel1FundamentalEvent;
        }

        /// <summary>
        /// Returns a timestamp for a tick converted to the exchange time zone
        /// </summary>
        private DateTime GetRealTimeTickTime(Symbol symbol)
        {
            var time = DateTime.UtcNow;
            var exchangeTimeZone = default(DateTimeZone);
            lock (_exchangeTimeZoneByLeanSymbol)
            {
                if (!_exchangeTimeZoneByLeanSymbol.TryGetValue(symbol, out exchangeTimeZone))
                {
                    // read the exchange time zone from market-hours-database
                    exchangeTimeZone = MarketHoursDatabase.FromDataFolder().GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType).TimeZone;
                    _exchangeTimeZoneByLeanSymbol[symbol] = exchangeTimeZone;
                }
            }

            return time.ConvertFromUtc(exchangeTimeZone);
        }

        private Symbol GetLeanSymbol(string ticker)
        {
            return _symbolUniverse.GetLeanSymbol(ticker, SecurityType.Base, null);
        }

        private void OnLevel1FundamentalEvent(object sender, Level1FundamentalEventArgs e)
        {
            // handle split data, they're only valid today, they'll show up around 4:45am EST
            if (e.SplitDate1.Date == DateTime.Today && DateTime.Now.TimeOfDay.TotalHours <= 8) // they will always be sent premarket
            {
                // get the last price, if it doesn't exist then we'll just issue the split claiming the price was zero
                // this should (ideally) never happen, but sending this without the price is much better then not sending
                // it at all
                double referencePrice;
                _prices.TryGetValue(e.Symbol, out referencePrice);

                var symbol = GetLeanSymbol(e.Symbol);
                var split = new Data.Market.Split(symbol, GetRealTimeTickTime(symbol), (decimal)referencePrice, (decimal)e.SplitFactor1, SplitType.SplitOccurred);
                Emit(split);
            }
        }

        /// <summary>
        /// Handle a new price update packet:
        /// </summary>
        private void OnLevel1SummaryUpdateEvent(object sender, Level1SummaryUpdateEventArgs e)
        {
            // if ticker is not found, unsubscribe
            if (e.NotFound) Unsubscribe(e.Symbol);

            // only update if we have a value
            if (e.Last == 0) return;

            // only accept trade and B/A updates
            if (e.TypeOfUpdate != Level1SummaryUpdateEventArgs.UpdateType.ExtendedTrade
             && e.TypeOfUpdate != Level1SummaryUpdateEventArgs.UpdateType.Trade
             && e.TypeOfUpdate != Level1SummaryUpdateEventArgs.UpdateType.Bid
             && e.TypeOfUpdate != Level1SummaryUpdateEventArgs.UpdateType.Ask) return;

            var last = (decimal)(e.TypeOfUpdate == Level1SummaryUpdateEventArgs.UpdateType.ExtendedTrade ? e.ExtendedTradingLast : e.Last);

            var symbol = GetLeanSymbol(e.Symbol);
            var time = GetRealTimeTickTime(symbol);

            TickType tradeType;

            switch (symbol.ID.SecurityType)
            {
                case SecurityType.Forex:
                    // TypeOfUpdate always equal to UpdateType.Trade for FXCM, but the message contains B/A and last data
                    tradeType = TickType.Quote;
                    break;
                default:
                    tradeType = e.TypeOfUpdate == Level1SummaryUpdateEventArgs.UpdateType.Bid ||
                                e.TypeOfUpdate == Level1SummaryUpdateEventArgs.UpdateType.Ask ?
                                TickType.Quote :
                                TickType.Trade;
                    break;
            }

            var tick = new Tick(GetRealTimeTickTime(symbol), symbol, last, (decimal)e.Bid, (decimal)e.Ask)
            {
                AskSize = e.AskSize,
                BidSize = e.BidSize,
                Quantity = e.IncrementalVolume,
                TickType = tradeType,
                DataType = MarketDataType.Tick
            };
            Emit(tick);
            _prices[e.Symbol] = e.Last;

            if (symbol.ID.SecurityType == SecurityType.Option || symbol.ID.SecurityType == SecurityType.Future)
            {
                if (!_openInterests.ContainsKey(e.Symbol) || _openInterests[e.Symbol] != e.OpenInterest)
                {
                    var oi = new OpenInterest(time, symbol, e.OpenInterest);
                    Emit(oi);

                    _openInterests[e.Symbol] = e.OpenInterest;
                }
            }
        }

        private void Emit(BaseData tick)
        {
            _aggregator.Update(tick);
        }

        /// <summary>
        /// Server has disconnected, reconnect.
        /// </summary>
        private void OnLevel1ServerDisconnected(object sender, Level1ServerDisconnectedArgs e)
        {
            Log.Error("IQFeed.OnLevel1ServerDisconnected(): LEVEL 1 PORT DISCONNECTED! " + e.TextLine);
        }

        /// <summary>
        /// Server has disconnected, reconnect.
        /// </summary>
        private void OnLevel1ServerReconnectFailed(object sender, Level1ServerReconnectFailedArgs e)
        {
            Log.Error("IQFeed.OnLevel1ServerReconnectFailed(): LEVEL 1 PORT DISCONNECT! " + e.TextLine);
        }

        /// <summary>
        /// Got a message we don't know about, log it for posterity.
        /// </summary>
        private void OnLevel1UnknownEvent(object sender, Level1TextLineEventArgs e)
        {
            Log.Error("IQFeed.OnUnknownEvent(): " + e.TextLine);
        }
    }

    // this type is expected to be used for exactly one job at a time
    public class HistoryPort : IQLookupHistorySymbolClient
    {
        private bool _inProgress;
        private readonly ConcurrentDictionary<string, HistoryRequest> _requestDataByRequestId = [];
        private ConcurrentDictionary<string, List<BaseData>> _currentRequest;
        private readonly IQFeedDataQueueUniverseProvider _symbolUniverse;

        /// <summary>
        /// Indicates whether the warning for invalid <see cref="SecurityType"/> has been fired.
        /// </summary>
        private bool _invalidSecurityTypeWarningFired;

        /// <summary>
        /// Indicates whether the warning for invalid <see cref="TickType.Quote"/> and <seealso cref="Resolution.Tick"/> has been fired.
        /// </summary>
        private bool _invalidTickTypeWithInvalidResolutionTypeWarningFired;

        /// <summary>
        /// Variable indicating whether a message for unsupported tick types has been logged to prevent log spam.
        /// </summary>
        private bool _unsupportedTickTypeMessagedLogged;

        /// <summary>
        /// ...
        /// </summary>
        public HistoryPort(IQFeedDataQueueUniverseProvider symbolUniverse)
            : base(IQFeedDefault.BufferSize)
        {
            _symbolUniverse = symbolUniverse;
            _currentRequest = new ConcurrentDictionary<string, List<BaseData>>();
        }

        /// <summary>
        /// ...
        /// </summary>
        public HistoryPort(IQFeedDataQueueUniverseProvider symbolUniverse, int maxDataPoints, int dataPointsPerSend)
            : this(symbolUniverse)
        {
            MaxDataPoints = maxDataPoints;
            DataPointsPerSend = dataPointsPerSend;
        }

        /// <summary>
        ///  Processes the specified historical data request and generates a sequence of <see cref="Slice"/> instances.
        /// </summary>
        /// <param name="request">The historical data requests</param>
        /// <param name="sliceTimeZone">The time zone used when time stamping the slice instances</param>
        /// <returns> An enumerable sequence of <see cref="Slice"/> objects representing the historical data for the request,
        /// or <c>null</c> if no data could be retrieved or processed.
        /// </returns>
        public IEnumerable<Slice>? ProcessHistoryRequests(HistoryRequest request, DateTimeZone sliceTimeZone)
        {
            // skipping universe and canonical symbols
            if (!CanHandle(request.Symbol) ||
                (request.Symbol.ID.SecurityType == SecurityType.Option && request.Symbol.IsCanonical()) ||
                (request.Symbol.ID.SecurityType == SecurityType.Future && request.Symbol.IsCanonical()))
            {
                if (!_invalidSecurityTypeWarningFired)
                {
                    Log.Trace($"{nameof(HistoryPort)}.{nameof(ProcessHistoryRequests)}: Unsupported SecurityType '{request.Symbol.ID.SecurityType}' for symbol '{request.Symbol}'");
                    _invalidSecurityTypeWarningFired = true;
                }
                return null;
            }

            if (request.TickType == TickType.OpenInterest)
            {
                if (!_unsupportedTickTypeMessagedLogged)
                {
                    Log.Trace($"{nameof(HistoryPort)}.{nameof(ProcessHistoryRequests)}: Unsupported tick type: {request.TickType}");
                    _unsupportedTickTypeMessagedLogged = true;
                }
                return null;
            }

            if (request.TickType == TickType.Quote && request.Resolution != Resolution.Tick)
            {
                if (!_invalidTickTypeWithInvalidResolutionTypeWarningFired)
                {
                    Log.Trace($"{nameof(HistoryPort)}.{nameof(ProcessHistoryRequests)}: Historical data request with TickType 'Quote' is not supported for resolutions other than Tick. Requested Resolution: {request.Resolution}");
                    _invalidTickTypeWithInvalidResolutionTypeWarningFired = true;
                }
                return null;
            }

            // Set this process status
            _inProgress = true;

            var ticker = _symbolUniverse.GetBrokerageSymbol(request.Symbol);
            var start = request.StartTimeUtc.ConvertFromUtc(IQFeedDataProvider.TimeZoneIQFeed);
            DateTime? end = request.EndTimeUtc.ConvertFromUtc(IQFeedDataProvider.TimeZoneIQFeed);
            // if we're within a minute of now, don't set the end time
            if (request.EndTimeUtc >= DateTime.UtcNow.AddMinutes(-1))
            {
                end = null;
            }

            Log.Trace($"HistoryPort.ProcessHistoryJob(): Submitting request: {request.Symbol.SecurityType.ToStringInvariant()}-{ticker}: " +
                $"{request.Resolution.ToStringInvariant()} {start.ToStringInvariant()}->{(end ?? DateTime.UtcNow.AddMinutes(-1)).ToStringInvariant()}"
            );

            int id;
            var reqid = string.Empty;

            switch (request.Resolution)
            {
                case Resolution.Tick:
                    id = RequestTickData(ticker, start, end, true);
                    reqid = CreateRequestID(LookupType.REQ_HST_TCK, id);
                    break;
                case Resolution.Daily:
                    id = RequestDailyData(ticker, start, end, true);
                    reqid = CreateRequestID(LookupType.REQ_HST_DWM, id);
                    break;
                default:
                    var interval = new Interval(GetPeriodType(request.Resolution), 1);
                    id = RequestIntervalData(ticker, interval, start, end, true);
                    reqid = CreateRequestID(LookupType.REQ_HST_INT, id);
                    break;
            }

            _requestDataByRequestId[reqid] = request;

            while (_inProgress)
            {
                continue;
            }

            return GetSlice(request.ExchangeHours.TimeZone, sliceTimeZone);
        }

        private IEnumerable<Slice>? GetSlice(DateTimeZone exchangeTz, DateTimeZone sliceTimeZone)
        {
            // After all data arrive, we pass it to the algorithm through memory and write to a file
            foreach (var key in _currentRequest.Keys)
            {
                if (_currentRequest.TryRemove(key, out var tradeBars))
                {
                    foreach (var tradeBar in tradeBars)
                    {
                        // Returns IEnumerable<Slice> object
                        yield return new Slice(tradeBar.EndTime.ConvertTo(exchangeTz, sliceTimeZone), new[] { tradeBar }, tradeBar.EndTime.ConvertToUtc(exchangeTz));
                    }
                }
            }
        }

        /// <summary>
        /// Returns true if this data provide can handle the specified symbol
        /// </summary>
        /// <param name="symbol">The symbol to be handled</param>
        /// <returns>True if this data provider can get data for the symbol, false otherwise</returns>
        private bool CanHandle(Symbol symbol)
        {
            var market = symbol.ID.Market;
            var securityType = symbol.ID.SecurityType;
            return
                (securityType == SecurityType.Equity && market == Market.USA) ||
                (securityType == SecurityType.Forex && market == Market.FXCM) ||
                (securityType == SecurityType.Option && market == Market.USA) ||
                (securityType == SecurityType.Future && IQFeedDataQueueUniverseProvider.FuturesExchanges.Values.Contains(market));
        }

        /// <summary>
        /// Created new request ID for a given lookup type (tick, intraday bar, daily bar)
        /// </summary>
        /// <param name="lookupType">Lookup type: REQ_HST_TCK (tick), REQ_HST_DWM (daily) or REQ_HST_INT (intraday resolutions)</param>
        /// <param name="id">Sequential identifier</param>
        /// <returns></returns>
        private static string CreateRequestID(LookupType lookupType, int id)
        {
            return lookupType + id.ToStringInvariant("0000000");
        }

        /// <summary>
        /// Method called when a new Lookup event is fired
        /// </summary>
        /// <param name="e">Received data</param>
        protected override void OnLookupEvent(LookupEventArgs e)
        {
            try
            {
                switch (e.Sequence)
                {
                    case LookupSequence.MessageStart:
                        _currentRequest.AddOrUpdate(e.Id, new List<BaseData>());
                        break;
                    case LookupSequence.MessageDetail:
                        if (_currentRequest.TryGetValue(e.Id, out var current))
                        {
                            HandleMessageDetail(e, current);
                        }
                        break;
                    case LookupSequence.MessageEnd:
                        _inProgress = false;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            catch (Exception err)
            {
                Log.Error(err);
            }
        }

        /// <summary>
        /// Put received data into current list of BaseData object
        /// </summary>
        /// <param name="e">Received data</param>
        /// <param name="current">Current list of BaseData object</param>
        private void HandleMessageDetail(LookupEventArgs e, List<BaseData> current)
        {
            var data = GetData(e, _requestDataByRequestId[e.Id]);
            if (data != null && data.Time != DateTime.MinValue)
            {
                current.Add(data);
            }
        }

        /// <summary>
        /// Transform received data into BaseData object
        /// </summary>
        /// <param name="e">Received data</param>
        /// <param name="requestData">Request information</param>
        /// <returns>BaseData object</returns>
        private BaseData GetData(LookupEventArgs e, HistoryRequest requestData)
        {
            try
            {
                switch (e.Type)
                {
                    case LookupType.REQ_HST_TCK:
                        if (e is LookupTickEventArgs t)
                        {
                            var tick = new Tick()
                            {
                                Time = t.DateTimeStamp.ConvertTo(IQFeedDataProvider.TimeZoneIQFeed, requestData.ExchangeHours.TimeZone),
                                DataType = MarketDataType.Tick,
                                Symbol = requestData.Symbol,
                            };

                            switch (requestData.TickType)
                            {
                                case TickType.Trade:
                                    tick.TickType = TickType.Trade;
                                    tick.Value = t.Last;
                                    tick.Quantity = t.LastSize;
                                    break;
                                case TickType.Quote:
                                    tick.TickType = TickType.Quote;
                                    tick.AskPrice = t.Ask;
                                    tick.BidPrice = t.Bid;
                                    break;
                                default:
                                    throw new NotImplementedException($"The TickType '{requestData.TickType}' is not supported in the {nameof(GetData)} method. Please implement the necessary logic for handling this TickType.");
                            }

                            return tick;
                        }
                        return null;
                    case LookupType.REQ_HST_INT:
                        if (e is LookupIntervalEventArgs i && i.DateTimeStamp != default)
                        {
                            var resolutionTimeSpan = requestData.Resolution.ToTimeSpan();
                            var time = (i.DateTimeStamp - resolutionTimeSpan).ConvertTo(IQFeedDataProvider.TimeZoneIQFeed, requestData.ExchangeHours.TimeZone);
                            return new TradeBar(time, requestData.Symbol, i.Open, i.High, i.Low, i.Close, i.PeriodVolume, resolutionTimeSpan);
                        }
                        return null;
                    case LookupType.REQ_HST_DWM:
                        if (e is LookupDayWeekMonthEventArgs d && d.DateTimeStamp != default)
                        {
                            return new TradeBar(d.DateTimeStamp.Date.ConvertTo(IQFeedDataProvider.TimeZoneIQFeed, requestData.ExchangeHours.TimeZone), requestData.Symbol, d.Open, d.High, d.Low, d.Close, d.PeriodVolume, requestData.Resolution.ToTimeSpan());
                        }
                        return null;
                    // we don't need to handle these other types
                    case LookupType.REQ_SYM_SYM:
                    case LookupType.REQ_SYM_SIC:
                    case LookupType.REQ_SYM_NAC:
                    case LookupType.REQ_TAB_MKT:
                    case LookupType.REQ_TAB_SEC:
                    case LookupType.REQ_TAB_MKC:
                    case LookupType.REQ_TAB_SIC:
                    case LookupType.REQ_TAB_NAC:
                    default:
                        return null;
                }
            }
            catch (Exception err)
            {
                Log.Error("Encountered error while processing request: " + e.Id);
                Log.Error(err);
                return null;
            }
        }

        private static PeriodType GetPeriodType(Resolution resolution)
        {
            switch (resolution)
            {
                case Resolution.Second:
                    return PeriodType.Second;
                case Resolution.Minute:
                    return PeriodType.Minute;
                case Resolution.Hour:
                    return PeriodType.Hour;
                case Resolution.Tick:
                case Resolution.Daily:
                default:
                    throw new ArgumentOutOfRangeException(nameof(resolution), resolution, null);
            }
        }
    }

    internal class IQFeedDefault
    {
        public static int BufferSize = 8192;
    }
}
