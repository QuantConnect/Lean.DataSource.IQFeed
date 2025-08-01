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
using NUnit.Framework;
using QuantConnect.Securities;
using System.Collections.Generic;
using QuantConnect.Configuration;

namespace QuantConnect.Lean.DataSource.IQFeed.Tests
{
    [TestFixture, Explicit("Requires locally installed and configured IQFeed client, including login credentials and active data subscription.")]
    public class IQFeedSymbolRepresentationTests
    {
        private IQFeedDataQueueUniverseProvider _dataQueueUniverseProvider;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            if (OS.IsWindows)
            {
                // IQConnect is only supported on Windows
                var connector = new IQConnect(Config.Get("iqfeed-productName"), "1.0");
                Assert.IsTrue(connector.Launch(), "Failed to launch IQConnect on Windows. Ensure IQFeed is installed and configured properly.");
            }

            _dataQueueUniverseProvider = new IQFeedDataQueueUniverseProvider();
        }

        [Test]
        public void ParseOptionIQFeedTicker()
        {
            // ticker contains two digits year of expiration
            var result = SymbolRepresentation.ParseOptionTickerIQFeed("MSFT1615D30");

            Assert.AreEqual(result.Underlying, "MSFT");
            Assert.AreEqual(result.OptionRight, OptionRight.Call);
            Assert.AreEqual(result.OptionStrike, 30m);
            Assert.AreEqual(result.ExpirationDate, new DateTime(2016, 4, 15));
        }

        [TestCase("MSFT1615D30.5", "MSFT", OptionRight.Call, "30.5", "20160415")]
        [TestCase("GOOG1415D30.5", "GOOG", OptionRight.Call, "30.5", "20140415")]
        [TestCase("GOOCV1415C30.5", "GOOCV", OptionRight.Call, "30.5", "20140315")]
        public void IQFeedTickerRoundTrip(string encodedOption, string underlying, OptionRight optionRight, decimal strike, string expiration)
        {
            var parsedOption = SymbolRepresentation.ParseOptionTickerIQFeed(encodedOption);

            Assert.AreEqual(underlying, parsedOption.Underlying);
            Assert.AreEqual(optionRight, parsedOption.OptionRight);
            Assert.AreEqual(strike, parsedOption.OptionStrike);
            Assert.AreEqual(QuantConnect.Time.ParseDate(expiration), parsedOption.ExpirationDate);

            var option = Symbol.CreateOption(parsedOption.Underlying, Market.USA, OptionStyle.American, parsedOption.OptionRight, parsedOption.OptionStrike, parsedOption.ExpirationDate);
            var result = SymbolRepresentation.GenerateOptionTicker(option);

            Assert.AreEqual(encodedOption, result);
        }

        public static IEnumerable<TestCaseData> GetFutureSymbolsTestCases()
        {
            // Natural gas futures expire the month previous to the contract month:
            // Expiry: August -> Contract month: September (U)
            yield return new TestCaseData("QNGU25", Symbol.CreateFuture(Futures.Energy.NaturalGas, Market.NYMEX, new DateTime(2025, 08, 27)));
            // Expiry: December 2025 -> Contract month: January (U) 2026 (26)
            yield return new TestCaseData("QNGF26", Symbol.CreateFuture(Futures.Energy.NaturalGas, Market.NYMEX, new DateTime(2025, 12, 29)));

            // BrentLastDayFinancial futures expire two months previous to the contract month:
            // Expiry: August -> Contract month: October (V)
            yield return new TestCaseData("QBZV25", Symbol.CreateFuture(Futures.Energy.BrentLastDayFinancial, Market.NYMEX, new DateTime(2025, 08, 29)));
            // Expiry: November 2025 -> Contract month: January (F) 2026 (26)
            yield return new TestCaseData("QBZF26", Symbol.CreateFuture(Futures.Energy.BrentLastDayFinancial, Market.NYMEX, new DateTime(2025, 11, 28)));
            // Expiry: December 2025 -> Contract month: February (G) 2026 (26)
            yield return new TestCaseData("QBZG26", Symbol.CreateFuture(Futures.Energy.BrentLastDayFinancial, Market.NYMEX, new DateTime(2025, 12, 31)));

            yield return new TestCaseData("@NQU25", Symbol.CreateFuture(Futures.Indices.NASDAQ100EMini, Market.CME, new DateTime(2025, 09, 19)));

            yield return new TestCaseData("@ADU25", Symbol.CreateFuture(Futures.Currencies.AUD, Market.CME, new DateTime(2025, 09, 15)))
                .SetDescription("IQFeed returns 'AD' for Australian Dollar; Lean expects '6A'. Verifies symbol mapping from 'IQFeed-symbol-map.json'.");

            yield return new TestCaseData("@BOU25", Symbol.CreateFuture(Futures.Grains.SoybeanOil, Market.CBOT, new DateTime(2025, 09, 12)))
                .SetDescription("Brokerage uses 'BO' for Soybean Oil; Lean maps it as 'ZL'. Validates symbol translation via 'IQFeed-symbol-map.json'.");
        }

        [TestCaseSource(nameof(GetFutureSymbolsTestCases))]
        public void ConvertsFutureSymbolRoundTrip(string brokerageSymbol, Symbol leanSymbol)
        {
            var actualBrokerageSymbol = _dataQueueUniverseProvider.GetBrokerageSymbol(leanSymbol);
            var actualLeanSymbol = _dataQueueUniverseProvider.GetLeanSymbol(brokerageSymbol, default, default);

            Assert.AreEqual(brokerageSymbol, actualBrokerageSymbol);
            Assert.AreEqual(leanSymbol, actualLeanSymbol);
        }

        [TestCase("NYMEX_GBX", "QQA", "QA")]
        [TestCase("NYMEX_GBX", "QQAN25", "QAN25")]
        public void NormalizeFuturesTickerRemovesFirstQForNymexGbxExchange(string exchange, string brokerageSymbol, string expectedNormalizedFutureSymbol)
        {
            var actualNormalizedFutureSymbol = IQFeedDataQueueUniverseProvider.NormalizeFuturesTicker(exchange, brokerageSymbol);
            Assert.AreEqual(expectedNormalizedFutureSymbol, actualNormalizedFutureSymbol);
        }
    }
}
