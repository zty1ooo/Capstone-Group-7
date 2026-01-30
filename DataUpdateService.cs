using CapstoneStockAnlyzer;
using Microsoft.Data.SqlClient;
using System.Collections.Concurrent;
using System.Data;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using static over_db;

public class DataUpdateService : BackgroundService
{
    private static DataTable? dtStocksDailyPriceClone = null;
    private static int LastDayUpdate = -1;

    public readonly ILogger<DataUpdateService> _logger;

    public DataUpdateService(ILogger<DataUpdateService> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)     //Data is updated everyday at 8:PM
    {
        await Task.Delay(1000, stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                int currentHour = Program.GetCentralTimeNow().Hour;
                if (LastDayUpdate != currentHour)
                {
                    LastDayUpdate = currentHour;
                    

                    if (currentHour == 20)
                    {
                        StartDailyPriceUpdate();
                    }

                    if (currentHour == 19)
                    {
                        UpdateTickerJSON();
                        Program.UpdateTickerStats();
                    }

                    
                }
                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in UpdateJSON");
            }
        }
    }

    public static void UpdateTickerJSON()
    {
        Program.dicTickerPricingJSON.Clear();
        DataTable? dt = null;
        if (FillDataTable_ViaSql(ref dt, "SELECT Ticker FROM StockTicker Order by Ticker") && dt != null && dt.Rows.Count > 0)
        {
            string sql = @"SELECT Ticker, StockDate, [Open], High, Low, [Close], AdjustedClose, Volume, DividendAmount, SplitCoefficient
                 FROM   StockDailyPrice
                 WHERE (Ticker = @Ticker)
                 ORDER BY StockDate";

            SqlCommand cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@Ticker", SqlDbType.VarChar, 10);

            foreach (DataRow dr in dt.Rows)
            {
                DataTable? dtStockPrices = null;
                cmd.Parameters["@Ticker"].Value = dr["Ticker"].ToString();
                if (FillDataTable_ViaCmd(ref dtStockPrices, ref cmd) && dtStockPrices != null && dtStockPrices.Rows.Count > 0)
                {
                    // If you want to handle specific data types or formatting:
                    var rowsWithTypeHandling = dtStockPrices.AsEnumerable().Select(row =>
                        dtStockPrices.Columns.Cast<DataColumn>().ToDictionary(
                            column => column.ColumnName,
                            column =>
                            {
                                var value = row[column];
                                if (value == DBNull.Value) return null;

                                // Handle specific types if needed
                                if (value is DateTime dt)
                                    return dt.ToString("s");
                                if (value is decimal dec)
                                    return dec.ToString("F4");

                                return value;
                            }
                        )
                    );

                    string tempJSONWithTypeHandling = JsonSerializer.Serialize(rowsWithTypeHandling, new JsonSerializerOptions
                    {
                        WriteIndented = false
                    });

                    string? Ticker = dr["Ticker"].ToString();

                    if (Ticker != null)
                        Program.dicTickerPricingJSON[Ticker] = tempJSONWithTypeHandling;


                    Console.WriteLine($"Ticker JSON update Complete: {Ticker}");
                }
            }
        }
    }

    public void StartDailyPriceUpdate()
    {
        if (dtStocksDailyPriceClone == null)
        {
            FillDataTable_ViaSql(ref dtStocksDailyPriceClone, "SELECT TOP (1) * FROM StockDailyPrice");
        }

        string sql = @"SELECT StockTicker.Ticker, ISNULL(tblLastStockDates.LastStockDate, '1/1/1970') AS LastStockDate, ISNULL(LastFullUpdate, '1/1/1970') AS LastFullUpdate, ISNULL(tblLastSplit.LastSplit, '1/1/1970') AS LastSplit
                 FROM   StockTicker LEFT OUTER JOIN
                                  (SELECT Ticker, MAX(StockDate) AS LastSplit
                                  FROM    StockDailyPrice AS StockDailyPrice_1
                                  WHERE (ISNULL(SplitCoefficient, 0) <> 0)
                                  GROUP BY Ticker) AS tblLastSplit ON StockTicker.Ticker = tblLastSplit.Ticker LEFT OUTER JOIN
                                  (SELECT Ticker, MAX(StockDate) AS LastStockDate
                                  FROM    StockDailyPrice
                                  GROUP BY Ticker) AS tblLastStockDates ON StockTicker.Ticker = tblLastStockDates.Ticker
                 ORDER BY StockTicker.Ticker";

        DataTable? dt = null;
        if (FillDataTable_ViaSql(ref dt, sql) && dt != null && dt.Rows.Count > 0)
        {
            List<Task> tasks = new List<Task>();
            int ThreadID = 1;
            foreach (DataRow dr in dt.Rows)
            {
                if (DateTime.TryParse(dr["LastStockDate"].ToString(), out DateTime out_LastStockDate))
                {
                    if (DateTime.TryParse(dr["LastFullUpdate"].ToString(), out DateTime out_LastFullUpdate))
                    {
                        if (DateTime.TryParse(dr["LastSplit"].ToString(), out DateTime out_LastSplit))
                        {
                            if (dr != null && !dr.IsNull("Ticker"))
                            {
                                int NewThreadID = ThreadID;
                                Task task = Task.Run(() => UpdatePrices(NewThreadID, dr["Ticker"].ToString(), out_LastStockDate, out_LastFullUpdate, out_LastSplit));
                                tasks.Add(task);
                            }
                            //UpdatePrices(ThreadID, dr["Ticker"].ToString(), out_LastStockDate, out_LastFullUpdate, out_LastSplit);
                            //Task.Run(() => UpdatePrices(ThreadID, dr["Ticker"].ToString(), out_LastStockDate, out_LastFullUpdate, out_LastSplit));
                        }
                    }
                }
                ThreadID++;
            }
            // Wait for all tasks to complete
            Task.WaitAll(tasks.ToArray());
            LogAndWriteLine($"All price updates completed!");

        }
    }

    private void UpdatePrices(int ThreadID, string? Ticker, DateTime LastStockDate, DateTime LastFullPricingUpdate, DateTime LastSplit)
    {
        LogAndWriteLine($"[{ThreadID}]{Ticker} Price Update Starting.");

        DataTable? dtNew = null;
        if (dtStocksDailyPriceClone != null)
            dtNew = dtStocksDailyPriceClone.Clone();

        // Compact or FullUpdate
        bool Compact = false;
        if (LastFullPricingUpdate.Year == 1970 || LastFullPricingUpdate < LastSplit || LastStockDate < DateTime.Now.AddDays(-20)) Compact = false; else Compact = true;
        // Get Start and End Dates
        string StartPeriod = "0";
        if (Compact) StartPeriod = ConvertToTimestamp(DateTime.Parse(DateTime.Now.ToString("d")).AddDays(-30)).ToString();
        string EndPeriod = ConvertToTimestamp(DateTime.Parse(DateTime.Now.ToString("d")).AddDays(+1)).ToString();

        string temp = "";
        temp = over_http.AnonDownload(ThreadID, "https://query2.finance.yahoo.com/v8/finance/chart/" + Ticker + "?symbol=" + Ticker + "&period1=" + StartPeriod + "&period2=" + EndPeriod + "&interval=1d&events=div%7Csplit");

        if (temp == "" || temp == "error" || temp.StartsWith("REMOTE_ADDR") || temp.IndexOf("\"code\":\"Not Found\"") > -1 || temp == "(404) Not Found")
        {
            return;
        }

        using (JsonDocument document = JsonDocument.Parse(temp))
        {
            JsonElement root = document.RootElement;

            // Check if chart.result[0] exists and get record count
            if (!root.TryGetProperty("chart", out JsonElement chart) ||
                !chart.TryGetProperty("result", out JsonElement result) ||
                result.GetArrayLength() == 0)
                return;

            JsonElement firstResult = result[0];

            if (!firstResult.TryGetProperty("timestamp", out JsonElement timestampArray))
                return;

            int RecordCount = timestampArray.GetArrayLength();
            if (RecordCount < 1) return;

            string StockDate = "", Open, High, Low, Close, AdjustedClose = "", Volume, DividendAmount, SplitCoefficient, Stamp, LastSplitDate = "1/1/1970";
            bool FirstDeleted = false;
            string StartBulk = "", EndBulk = "";

            // Get indicators structure
            JsonElement indicators = new JsonElement();
            JsonElement adjcloseArray = new JsonElement();
            JsonElement quoteArray = new JsonElement();
            JsonElement adjcloseValues = new JsonElement();
            JsonElement quoteValues = new JsonElement();

            bool hasIndicators = firstResult.TryGetProperty("indicators", out indicators);
            bool hasAdjclose = hasIndicators &&
                               indicators.TryGetProperty("adjclose", out adjcloseArray) &&
                               adjcloseArray.GetArrayLength() > 0 &&
                               adjcloseArray[0].TryGetProperty("adjclose", out adjcloseValues);
            bool hasQuote = hasIndicators &&
                            indicators.TryGetProperty("quote", out quoteArray) &&
                            quoteArray.GetArrayLength() > 0;
            if (hasQuote)
                quoteValues = quoteArray[0];

            // Get events structure
            JsonElement events = new JsonElement();
            JsonElement dividends = new JsonElement();
            JsonElement splits = new JsonElement();
            bool hasEvents = firstResult.TryGetProperty("events", out events);
            bool hasDividends = hasEvents && events.TryGetProperty("dividends", out dividends);
            bool hasSplits = hasEvents && events.TryGetProperty("splits", out splits);

            for (int i = 0; i < RecordCount; i++)
            {
                try
                {
                    // Get timestamp
                    if (timestampArray[i].ValueKind == JsonValueKind.Null)
                        continue;
                    Stamp = timestampArray[i].ToString();
                    StockDate = UnixTimeStampToDateTime(long.Parse(Stamp)).ToString("d");

                    // Get adjusted close
                    AdjustedClose = "";
                    if (hasAdjclose && i < adjcloseValues.GetArrayLength() &&
                        adjcloseValues[i].ValueKind != JsonValueKind.Null)
                    {
                        AdjustedClose = adjcloseValues[i].ToString();
                    }

                    // Get quote data
                    Open = "";
                    High = "";
                    Low = "";
                    Close = "";
                    Volume = "";

                    if (hasQuote)
                    {
                        if (quoteValues.TryGetProperty("open", out JsonElement openArray) &&
                            i < openArray.GetArrayLength() &&
                            openArray[i].ValueKind != JsonValueKind.Null)
                        {
                            Open = openArray[i].ToString();
                        }

                        if (quoteValues.TryGetProperty("high", out JsonElement highArray) &&
                            i < highArray.GetArrayLength() &&
                            highArray[i].ValueKind != JsonValueKind.Null)
                        {
                            High = highArray[i].ToString();
                        }

                        if (quoteValues.TryGetProperty("low", out JsonElement lowArray) &&
                            i < lowArray.GetArrayLength() &&
                            lowArray[i].ValueKind != JsonValueKind.Null)
                        {
                            Low = lowArray[i].ToString();
                        }

                        if (quoteValues.TryGetProperty("close", out JsonElement closeArray) &&
                            i < closeArray.GetArrayLength() &&
                            closeArray[i].ValueKind != JsonValueKind.Null)
                        {
                            Close = closeArray[i].ToString();
                        }

                        if (quoteValues.TryGetProperty("volume", out JsonElement volumeArray) &&
                            i < volumeArray.GetArrayLength() &&
                            volumeArray[i].ValueKind != JsonValueKind.Null)
                        {
                            Volume = volumeArray[i].ToString();
                        }
                    }

                    DividendAmount = "0.0000";
                    SplitCoefficient = "0.0000";

                    // Check for dividends
                    if (hasDividends && dividends.TryGetProperty(Stamp, out JsonElement dividend))
                    {
                        if (dividend.TryGetProperty("amount", out JsonElement amount) &&
                            amount.ValueKind != JsonValueKind.Null)
                        {
                            DividendAmount = amount.ToString();
                        }
                    }

                    // Check for splits
                    if (hasSplits && splits.TryGetProperty(Stamp, out JsonElement split))
                    {
                        LastSplitDate = StockDate;

                        double numerator = 1.0;
                        double denominator = 1.0;

                        if (split.TryGetProperty("numerator", out JsonElement num) &&
                            num.ValueKind != JsonValueKind.Null)
                        {
                            double.TryParse(num.ToString(), out numerator);
                        }

                        if (split.TryGetProperty("denominator", out JsonElement den) &&
                            den.ValueKind != JsonValueKind.Null)
                        {
                            double.TryParse(den.ToString(), out denominator);
                        }

                        SplitCoefficient = (numerator / denominator).ToString("F4");
                    }

                    // Clean Up old data on first pass
                    if (!FirstDeleted)
                    {
                        SqlCommand cmd = new SqlCommand("DELETE from StockDailyPrice where ticker = @Ticker and StockDate >= @StockDate");
                        cmd.Parameters.Add("@Ticker", SqlDbType.VarChar, 10).Value = Ticker;
                        cmd.Parameters.Add("@StockDate", SqlDbType.Date).Value = StockDate;
                        if (ExecSqlCommand(ref cmd))
                        {
                            FirstDeleted = true;
                            StartBulk = StockDate;
                        }
                        else
                            return;
                    }

                    if ((!(DateTime.Now.Hour < 17 && DateTime.Parse(StockDate) == DateTime.Parse(DateTime.Now.ToString("d")))) && EndBulk != StockDate)
                    {
                        // Only add row if we have the required data
                        if (!string.IsNullOrEmpty(Open) && !string.IsNullOrEmpty(High) &&
                            !string.IsNullOrEmpty(Low) && !string.IsNullOrEmpty(Close) &&
                            !string.IsNullOrEmpty(AdjustedClose))
                        {
                            if (dtNew != null)
                            {
                                DataRow dr = dtNew.NewRow();
                                dr[0] = Ticker;
                                dr[1] = StockDate;
                                dr[2] = decimal.Parse(Open, NumberStyles.Float);
                                dr[3] = decimal.Parse(High, NumberStyles.Float);
                                dr[4] = decimal.Parse(Low, NumberStyles.Float);
                                dr[5] = decimal.Parse(Close, NumberStyles.Float);
                                dr[6] = decimal.Parse(AdjustedClose, NumberStyles.Float);
                                dr[7] = Volume;
                                dr[8] = DividendAmount;
                                dr[9] = SplitCoefficient;
                                dtNew.Rows.Add(dr);
                                EndBulk = StockDate;
                            }

                        }
                    }
                }
                catch (Exception)
                {
                    continue;
                }
            }
        }

        if (dtNew != null && dtNew.Rows.Count > 0)
        {
            dtNew = RemoveDuplicateRows(dtNew, "Stockdate");
            if (BulkInsertDataTable(ref dtNew, "StockDailyPrice"))
            {
                string fullupdateSQL = "";
                if (!Compact) fullupdateSQL = "LastFullUpdate = GETDATE(),";
                string sql = @$"UPDATE StockTicker
                     SET       {fullupdateSQL} DailyPriceUpdateInProcess = 0, DailyPriceUpdateComplete = 1
                     WHERE (Ticker = @Ticker)";
                SqlCommand cmd = new SqlCommand(sql);
                cmd.Parameters.Add("@Ticker", SqlDbType.VarChar, 10).Value = Ticker;
                if (ExecSqlCommand(ref cmd))
                {
                    LogAndWriteLine($"[{ThreadID}]{Ticker} Price Update Complete. Records: {dtNew.Rows.Count}, Compact: {Compact}");
                }
            }
        }
    }


    public void LogAndWriteLine(string message)
    {
        _logger.LogInformation($"{message} at: {Program.GetCentralTimeNow().ToString("M/d/yyyy h:mm tt")}");
    }
}
