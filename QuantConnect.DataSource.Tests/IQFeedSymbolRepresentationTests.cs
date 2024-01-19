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

namespace QuantConnect.DataSource.Tests
{
    [TestFixture]
    public class IQFeedSymbolRepresentationTests
    {
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
    }
}
