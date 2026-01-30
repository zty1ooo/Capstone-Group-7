using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.AspNetCore.ResponseCompression;
using System.Collections.Concurrent;
using System.Data;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Threading.RateLimiting;
using static over_db;

namespace CapstoneStockAnlyzer
{
    public class Program
    {
        public static readonly TimeZoneInfo CentralTimeZone = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time") : TimeZoneInfo.FindSystemTimeZoneById("America/Chicago");
        public static ConcurrentDictionary<string, string> dicTickerPricingJSON = new ConcurrentDictionary<string, string>();
        public static DataTable? dtTickerStats = null;
        public static string TickerStatsHTML = "";
        public static ConcurrentDictionary<string, string> dicTickerStream = new ConcurrentDictionary<string, string>();

        public static void Main(string[] args)
        {

            DataUpdateService.UpdateTickerJSON();
            UpdateTickerStats();

            var builder = WebApplication.CreateBuilder(args);
            builder.WebHost.ConfigureKestrel(options =>
            {
                options.ListenAnyIP(8080); // HTTP
                                           // options.ListenAnyIP(8080, listenOptions => listenOptions.UseHttps("cert.pfx", "password")); // For HTTPS
            });

            // Add logging
            builder.Services.AddLogging(logging => logging.AddConsole());

            // Add response compression
            builder.Services.AddResponseCompression(options =>
            {
                options.EnableForHttps = true;
                options.Providers.Add<GzipCompressionProvider>();
                options.MimeTypes = ResponseCompressionDefaults.MimeTypes.Concat(new[] { "application/json" });
            });

            // Configure rate limiting
            builder.Services.AddRateLimiter(options =>
            {
                options.AddSlidingWindowLimiter("Api", opt =>
                {
                    opt.PermitLimit = 75;
                    opt.Window = TimeSpan.FromSeconds(30);
                    opt.SegmentsPerWindow = 6;
                    opt.QueueProcessingOrder = QueueProcessingOrder.OldestFirst;
                    opt.QueueLimit = 10;
                });
            });

            // Add TopHourUpdateService
            builder.Services.AddHostedService<DataUpdateService>();

            // Configure Kestrel
            builder.WebHost.ConfigureKestrel(options =>
            {
                options.Limits.KeepAliveTimeout = TimeSpan.FromSeconds(180);
                options.Limits.RequestHeadersTimeout = TimeSpan.FromSeconds(180);
            });

            var app = builder.Build();

            if (!app.Environment.IsDevelopment())
            {
                app.UseExceptionHandler("/Error");
                app.UseHsts();
            }

            // Security headers with updated CSP
            app.Use(async (context, next) =>
            {
                context.Response.Headers.XContentTypeOptions = "nosniff";
                context.Response.Headers["X-XSS-Protection"] = "1; mode=block";
                context.Response.Headers.XFrameOptions = "DENY";
                context.Response.Headers.ContentSecurityPolicy =
                    "default-src 'none'; script-src 'self' https://cdn.jsdelivr.net/ https://ajax.googleapis.com/; style-src 'self' https://cdn.jsdelivr.net/; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none';";
                await next(context);
            });

            app.UseResponseCompression();
            app.UseRateLimiter();

            // Root endpoint
            app.MapGet("/", (HttpContext context) =>
            {
                context.Response.Headers.CacheControl = "no-cache";
                context.Response.Headers.Expires = "-1";

                DateTime now = GetCentralTimeNow();
                TimeZoneInfo? centralZone = CentralTimeZone;

                string htmlTable = @"<link href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.7/dist/css/bootstrap.min.css' rel='stylesheet' integrity='sha384-LN+7fdVzj6u52u30Kp6M/trliBMCMKTyK833zpbD+pXdCLuTusPj697FH4R/5mcr' crossorigin='anonymous'>
         <script src='https://cdn.jsdelivr.net/npm/bootstrap@5.3.7/dist/js/bootstrap.bundle.min.js' integrity='sha384-ndDqU0Gzau9qJ1lfW4pNLlhNTkCfHzAVBReH9diLvGRem5+R9g2FzA8ZGN954O5Q' crossorigin='anonymous'></script>
         <div class='container mt-3'>
         <table class='table table-bordered table-sm'>
             <tbody>
                 <tr>
                     <td>Server Current Date and Time</td>
                     <td>" + now.ToString("M/d/yyyy h:mm tt") + @"</td>
                 </tr>
                 <tr>
                     <td>Timezone</td>
                     <td>" + (centralZone == null ? "Error" : centralZone.DisplayName) + @"</td>
                 </tr>
                 <tr>
                     <td>Timezone ID</td>
                     <td>" + (centralZone == null ? "Error" : centralZone.Id) + @"</td>
                 </tr>
                 <tr>
                     <td>Is Daylight Saving Time</td>
                     <td>" + (centralZone == null ? false : centralZone.IsDaylightSavingTime(now)) + @"</td>
                 </tr>
                 <tr>
                     <td>UTC Offset</td>
                     <td>" + (centralZone == null ? "Error" : centralZone.GetUtcOffset(now).ToString()) + $@"</td>
                 </tr>
             </tbody>
         </table>
        
            <b>StockPrice Datasets: <a href='{context.Request.GetDisplayUrl() + "stockprices?ticker=SPY"}'>{context.Request.GetDisplayUrl() + "stockprices?ticker=SPY"}</a></b>
            " + TickerStatsHTML + @"
         </div>";

                return Results.Text(
                    $"<!DOCTYPE html><html><head><title>AlgoArmada API</title></head><body><div class='container mt-3'><h3>Capstone Group 7 Stock Analyzer</h3>{htmlTable}</div></body></html>",
                    "text/html; charset=utf-8"
                );
            }).RequireRateLimiting("Api");

            // Stock endpoint
            app.MapGet("/stockprices/", async (HttpContext context, ILogger<Program> logger) =>
            {
                bool ReturnAll = false;
                if (!string.IsNullOrEmpty(context.Request.Query["favmascot"]) && (context.Request.Query["favmascot"].ToString().ToUpper() == "RAZORBACK" || context.Request.Query["favmascot"].ToString().ToUpper() == "RAZORBACKS" || context.Request.Query["favmascot"].ToString().ToUpper() == "HOG" || context.Request.Query["favmascot"].ToString().ToUpper() == "HOGS"))
                    ReturnAll = true;

                string? Ticker = "ALL";
                if (!string.IsNullOrEmpty(context.Request.Query["ticker"]))
                    Ticker = context.Request.Query["ticker"].ToString().ToUpper();

                ConfigureJSONResponseHeaders(context.Response);
                logger.LogInformation("Starting Stock Pricing Request at {Time}, IP: {IP}",
                    GetCentralTimeNow().ToString("M/d/yyyy h:mm tt"), context.Connection.RemoteIpAddress);

                if (Ticker == "ALL" && Program.dicTickerPricingJSON.Count > 0 && !Program.dicTickerPricingJSON.ContainsKey("ALL"))
                {
                    List<string> lstALL = new List<string>();
                    foreach (var item in Program.dicTickerPricingJSON)
                    {
                        lstALL.Add(item.Value.Substring(1, item.Value.Length - 2));
                    }
                    Program.dicTickerPricingJSON["ALL"] = "[" + string.Join(',', lstALL) + "]";
                }

                var timeout = GetCentralTimeNow().AddSeconds(100);
                while (Ticker != null && (!Program.dicTickerPricingJSON.ContainsKey(Ticker) || Program.dicTickerPricingJSON[Ticker] == "") && GetCentralTimeNow() < timeout)
                {
                    await Task.Delay(1000, context.RequestAborted);
                }

                if (Ticker != null && (!Program.dicTickerPricingJSON.ContainsKey(Ticker) || Program.dicTickerPricingJSON[Ticker] == ""))
                {
                    logger.LogWarning($"Timeout waiting for JSON data at {GetCentralTimeNow().ToString("M/d/yyyy h:mm tt")}");
                    return Results.StatusCode(503);
                }

                try
                {
                    if (Ticker != null && Program.dicTickerPricingJSON.ContainsKey(Ticker))
                        JsonDocument.Parse(Program.dicTickerPricingJSON[Ticker]);
                    else
                        return Results.StatusCode(500);
                }
                catch (JsonException ex)
                {
                    logger.LogError(ex, $"Invalid JSON in Program.dicTickerPricingJSON for Ticker:{Ticker} at {GetCentralTimeNow().ToString("M/d/yyyy h:mm tt")}");
                    over_db.SendErrorEmail("MainMethodError",
                        $"Invalid JSON in dicTickerPricingJSON at {GetCentralTimeNow().ToString("M/d/yyyy h:mm tt")}", ex);
                    return Results.StatusCode(500);
                }

                logger.LogInformation($"Streaming Ticker: [{Ticker}] Pricing data at {GetCentralTimeNow().ToString("M/d/yyyy h:mm tt")}, size: {Program.dicTickerPricingJSON[Ticker].Length} bytes");

                if (ReturnAll)
                {
                    //var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(Program.dicTickerPricingJSON[Ticker]));
                    return Results.Stream(new MemoryStream(System.Text.Encoding.UTF8.GetBytes(Program.dicTickerPricingJSON[Ticker])), "application/json; charset=utf-8");
                }
                else
                {
                    if (!Program.dicTickerStream.ContainsKey(Ticker))
                    {
                        Program.dicTickerStream[Ticker] = JsonSerializer.Serialize(JsonDocument.Parse(Program.dicTickerPricingJSON[Ticker]).RootElement.EnumerateArray().TakeLast(100));
                    }
                    return Results.Stream(new MemoryStream(System.Text.Encoding.UTF8.GetBytes(Program.dicTickerStream[Ticker])), "application/json; charset=utf-8");
                }

                //logger.LogWarning($"Unauthorized dicTickerPricingJSON request at {GetCentralTimeNow().ToString("M/d/yyyy h:mm tt")}, IP: {context.Connection.RemoteIpAddress}");
                //return Results.StatusCode(403);
            }).RequireRateLimiting("Api");

            app.Run();
        }

        private static void ConfigureJSONResponseHeaders(HttpResponse response)
        {
            response.ContentType = "application/json; charset=utf-8";
            response.Headers.CacheControl = "no-cache";
            response.Headers["Expires"] = "-1";
        }

        public static DateTime GetCentralTimeNow()
        {
            return TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, CentralTimeZone);
        }

        public static void UpdateTickerStats()
        {
            string sql = @"SELECT StockTicker.Ticker, ISNULL(tblLastStockDate.LastStockDate, '1/1/1970') AS LastStockDate, ISNULL(tblLastStockDate.RecordCount, 0) AS RecordCount
        FROM   StockTicker LEFT OUTER JOIN
                         (SELECT Ticker, MAX(StockDate) AS LastStockDate, COUNT(StockDate) AS RecordCount
                         FROM    StockDailyPrice
                         GROUP BY Ticker) AS tblLastStockDate ON StockTicker.Ticker = tblLastStockDate.Ticker
        ORDER BY StockTicker.Ticker";

            FillDataTable_ViaSql(ref Program.dtTickerStats, sql);

            // Build the ticker stats table
            StringBuilder tickerStatsTable = new StringBuilder();
            tickerStatsTable.Append(@"
        <table class='table table-bordered table-sm table-hover'>
            <thead>
                <tr>
                    <th class='text-start'>Ticker</th>
                    <th class='text-start'>Last Stock Date</th>
                    <th class='text-start'>Record Count</th>
                </tr>
            </thead>
            <tbody>");

            // Add rows from dtTickerStats
            if (dtTickerStats != null && dtTickerStats.Rows.Count > 0)
            {
                foreach (DataRow row in dtTickerStats.Rows)
                {
                    string ticker = row["Ticker"]?.ToString() ?? "N/A";
                    string lastStockDate = "";
                    string recordCount = row["RecordCount"]?.ToString() ?? "0";

                    // Format the date if it's valid
                    if (DateTime.TryParse(row["LastStockDate"]?.ToString(), out DateTime lastDate))
                    {
                        lastStockDate = lastDate.ToString("M/d/yyyy");
                    }
                    else
                    {
                        lastStockDate = "N/A";
                    }

                    tickerStatsTable.Append($@"
            <tr>
                <td>{ticker}</td>
                <td>{lastStockDate}</td>
                <td>{recordCount}</td>
            </tr>");
                }

                tickerStatsTable.Append(@"
            </tbody>
        </table>");
            }


            TickerStatsHTML = tickerStatsTable.ToString();
        }
    }
}
