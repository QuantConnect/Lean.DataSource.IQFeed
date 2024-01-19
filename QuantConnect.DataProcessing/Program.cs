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
using QuantConnect.Logging;
using QuantConnect.Configuration;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// Entry point for the data downloader/converter
    /// </summary>
    public class Program
    {
        /// <summary>
        /// Entry point of the program
        /// </summary>
        /// <returns>Exit code. 0 equals successful, and any other value indicates the downloader/converter failed.</returns>
        public static void Main(string[] args)
        {
            try
            {
                var optionsObject = ToolboxArgumentParser.ParseArguments(args);

                var tickers = ToolboxArgumentParser.GetTickers(optionsObject);
                var resolution = optionsObject.ContainsKey("resolution") ? optionsObject["resolution"].ToString() : "";
                var fromDate = Parse.DateTimeExact(optionsObject["from-date"].ToString(), "yyyyMMdd-HH:mm:ss");
                var toDate = optionsObject.ContainsKey("to-date")
                    ? Parse.DateTimeExact(optionsObject["to-date"].ToString(), "yyyyMMdd-HH:mm:ss") : DateTime.UtcNow;

                // Run the data downloader/converter.
                IQFeedDownloaderProgram.IQFeedDownloader(tickers, resolution, fromDate, toDate);
            }
            catch (Exception err)
            {
                Log.Error(err, "The downloader/converter for data exited unexpectedly");
                Environment.Exit(1);
            }
            
            // The downloader/converter was successful
            Environment.Exit(0);
        }
    }
}