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
using QuantConnect.Securities;

namespace QuantConnect.Lean.DataSource.IQFeed.Models;

/// <summary>
/// Represents a subset of parameters extracted from a history request,
/// providing context for historical data queries related to a specific symbol.
/// </summary>
public class HistoryRequestContext
{
    /// <summary>
    /// Gets the symbol for which historical data is requested.
    /// </summary>
    public Symbol Symbol { get; }

    /// <summary>
    /// Gets the type of tick data requested (e.g., trade, quote).
    /// </summary>
    public TickType TickType { get; }

    /// <summary>
    /// Gets the resolution of the historical data request as a <see cref="TimeSpan"/>.
    /// </summary>
    public TimeSpan Resolution { get; }

    /// <summary>
    /// Gets the time zone associated with the symbol's exchange.
    /// </summary>
    public DateTimeZone SymbolDateTimeZone { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="HistoryRequestContext"/> class,
    /// setting symbol, tick type, resolution, and determining the symbol's exchange time zone.
    /// </summary>
    /// <param name="symbol">The symbol representing the financial instrument.</param>
    /// <param name="tickType">The tick data type (trade, quote, etc.).</param>
    /// <param name="resolution">The resolution of the requested historical data.</param>
    public HistoryRequestContext(Symbol symbol, TickType tickType, Resolution resolution)
    {
        Symbol = symbol;
        TickType = tickType;
        Resolution = resolution.ToTimeSpan();
        SymbolDateTimeZone = MarketHoursDatabase.FromDataFolder().GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType).TimeZone;
    }
}
