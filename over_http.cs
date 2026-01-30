using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Text.Json;

public class over_http
{
    const int HttpTimeOut = 15;
    private static bool InitInProcess = true;
    private static DateTime LastRequest = DateTime.Now.AddSeconds(-5);

    private static HttpClientHandler handler = new HttpClientHandler() { UseCookies = true, AutomaticDecompression = DecompressionMethods.All, ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator };
    private static HttpClient client = new HttpClient(handler, true) { Timeout = TimeSpan.FromSeconds(HttpTimeOut) };

    private static ConcurrentDictionary<string, HttpClient> dicClient = new ConcurrentDictionary<string, HttpClient>();
    private static ConcurrentDictionary<string, HttpClientHandler> dicHandler = new ConcurrentDictionary<string, HttpClientHandler>();
    private static ConcurrentDictionary<string, WebProxy> dicProxy = new ConcurrentDictionary<string, WebProxy>();

    private static ConcurrentQueue<string> queProxies = new ConcurrentQueue<string>();
    private static ConcurrentStack<string> stackProxies = new ConcurrentStack<string>();

    private static ConcurrentDictionary<int, string> dicUAs = new ConcurrentDictionary<int, string>() { [1] = "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.111 Mobile Safari/537.36", [2] = "Mozilla/5.0 (iPhone; CPU iPhone OS 13_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Mobile/15E148 Safari/604.1", [3] = "Mozilla/5.0 (Android 10; Mobile; rv:68.0) Gecko/68.0 Firefox/68.0", [4] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36", [5] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36", [6] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/114.0" };
    private static DateTime LastUAUpdate = DateTime.Parse("3/26/2003");

    private static string[] Langs = { "en-GB,en-US;q=0.9,en;q=0.8", "en-US,en;q=0.9" };
    private static string[] Accepts = { "text/html, application/xhtml+xml, image/jxr, */*", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9" };

    private static Random random = new Random();
    private static List<string>? Proxies = null;
    private static DateTime LastProxyUpdate = DateTime.Parse("3/2/1976");
    private static ConcurrentDictionary<string, string> dicCrumb = new ConcurrentDictionary<string, string>();


    static over_http()
    {
        Task.Run(() => Init());
    }

    public static bool Init()
    {
        while (true)
        {
            // Update UAs if older than 1 day
            if ((DateTime.Now - LastUAUpdate).TotalDays > 1)
            {
                InitInProcess = true;
                UpdateUserAgents();
                if (Proxies != null)
                    InitInProcess = false;
            }
            // Update Proxies if older than 1 day
            if ((DateTime.Now - LastProxyUpdate).TotalHours > 1)
            {
                InitInProcess = true;
                UpdateProxies();
                InitInProcess = false;
            }
            Thread.Sleep(60000);
        }
    }

    private static bool MakeNewClient(string ProxyID)
    {
        try
        {
            if (ProxyID.IndexOf(':') > 0)
            {
                string[] ProxyAddressAndPort = ProxyID.Split(':');
                if (ProxyAddressAndPort.Length == 2)
                {
                    UriBuilder uriBuilder = new UriBuilder();
                    uriBuilder.Host = ProxyAddressAndPort[0];
                    if (int.TryParse(ProxyAddressAndPort[1], out int lo_port))
                    {
                        try
                        {
                            uriBuilder.Port = lo_port;
                            dicProxy[ProxyID] = new WebProxy() { BypassProxyOnLocal = false, UseDefaultCredentials = false };
                            dicProxy[ProxyID].Address = uriBuilder.Uri;
                            dicHandler[ProxyID] = new HttpClientHandler() { Proxy = dicProxy[ProxyID], UseProxy = true, UseCookies = false, AutomaticDecompression = DecompressionMethods.All, ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator };
                            dicClient[ProxyID] = new HttpClient(dicHandler[ProxyID], true) { Timeout = TimeSpan.FromSeconds(HttpTimeOut) };
                            dicClient[ProxyID].DefaultRequestHeaders.Add("Accept", Accepts[GetRand(0, Accepts.Length - 1)]);
                            dicClient[ProxyID].DefaultRequestHeaders.Add("Accept-Encoding", "gzip, deflate");
                            dicClient[ProxyID].DefaultRequestHeaders.Add("Accept-Language", Langs[GetRand(0, Langs.Length - 1)]);
                            dicClient[ProxyID].DefaultRequestHeaders.Add("Cache-Control", "no-cache");
                            dicClient[ProxyID].DefaultRequestHeaders.Add("Referer", "https://finance.yahoo.com");
                            dicClient[ProxyID].DefaultRequestHeaders.Add("Upgrade-Insecure-Requests", "1");
                            dicClient[ProxyID].DefaultRequestHeaders.Add("User-Agent", dicUAs[GetRand(1, dicUAs.Count)]);
                            return true;
                        }
                        catch (Exception)
                        {
                            return false;
                        }
                    }
                    else
                        return false;
                }
                else
                    return false;
            }
            else
                return false;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public static string AnonDownload(int ThreadID, string URL)
    {
        while (InitInProcess)
            Thread.Sleep(1000);

        string? ClientID = "";
        int Retries = 0;

        while (true)
        {
            //Thread.Sleep(500);
            try
            {
                if (ClientID == "")
                {
                    bool done = false;
                    while (!done)
                    {
                        if (stackProxies.IsEmpty)
                        {
                            // No Verified Good client Available, get an unknown from que
                            if (Proxies != null && queProxies.IsEmpty)
                            {
                                // Que is empty, refill from master proxy list                                
                                lock (threadLock)
                                {
                                    foreach (string proxy in Proxies)
                                    {
                                        if (!queProxies.Contains(proxy) && !stackProxies.Contains(proxy)) queProxies.Enqueue(proxy);
                                    }
                                }
                            }
                            // try and get client from que
                            if (queProxies.TryDequeue(out ClientID))
                                done = true;
                        }
                        else
                        {
                            // Verified Good Proxy Available, try and get it
                            if (stackProxies.TryPop(out ClientID))
                                done = true;
                        }
                    }
                }

                Debug.WriteLine($"[Q:{queProxies.Count},S:{stackProxies.Count}]({ThreadID}) {URL}");

                // Try and Download
                try
                {
                    string URL2Use = URL;
                    if (ClientID != null && dicCrumb.ContainsKey(ClientID) && dicCrumb[ClientID] != "")
                    {
                        URL2Use = URL + "&crumb=" + dicCrumb[ClientID];
                    }

                    if (ClientID != null)
                    {
                        using (HttpResponseMessage IISresponse = dicClient[ClientID].GetAsync(URL2Use).Result)
                        {
                            //IISresponse.EnsureSuccessStatusCode(); // Throw if httpcode is an error
                            using (HttpContent content = IISresponse.Content)
                            {
                                if (IISresponse.StatusCode == HttpStatusCode.OK && content.ReadAsStringAsync().Result.StartsWith("{") /*!content.ReadAsStringAsync().Result.ToUpper().StartsWith("REMOTE_ADDR")*/)
                                {
                                    Thread.Sleep(GetRand(2000, 6000));
                                    if (!stackProxies.Contains(ClientID)) stackProxies.Push(ClientID);
                                    return content.ReadAsStringAsync().Result;
                                }
                                else
                                {
                                    string ErrorText = content.ReadAsStringAsync().Result;
                                    if (URL.ToUpper().Contains("YAHOO") && (ErrorText.ToUpper().Contains("COOKIE") || ErrorText.ToUpper().Contains("CRUMB")))
                                    {
                                        using (HttpResponseMessage IISresponse2 = dicClient[ClientID].GetAsync("https://fc.yahoo.com/").Result)
                                        {
                                            using (HttpContent content2 = IISresponse2.Content)
                                            {
                                                string YahooErrorText = content2.ReadAsStringAsync().Result;
                                                if (IISresponse2.Headers.TryGetValues("Set-Cookie", out IEnumerable<string>? tempCookie))
                                                {
                                                    dicClient[ClientID].DefaultRequestHeaders.Remove("Cookie");
                                                    dicClient[ClientID].DefaultRequestHeaders.Add("Cookie", tempCookie.First());
                                                    using (HttpResponseMessage IISresponse3 = dicClient[ClientID].GetAsync("https://query2.finance.yahoo.com/v1/test/getcrumb").Result)
                                                    {
                                                        using (HttpContent content3 = IISresponse3.Content)
                                                        {
                                                            string YahooCrumb = content3.ReadAsStringAsync().Result;
                                                            dicCrumb[ClientID] = YahooCrumb;
                                                            Thread.Sleep(GetRand(1000, 2000));
                                                            continue;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                    }


                }
                catch (Exception)
                {
                }

                if (ClientID != null && dicCrumb.ContainsKey(ClientID)) dicCrumb[ClientID] = "";
                ClientID = "";

                Retries++;
                if (Retries >= 15) return "error";
            }
            catch (Exception ex)
            {
                if (ClientID != "")
                {
                    if (ClientID != null && dicCrumb.ContainsKey(ClientID)) dicCrumb[ClientID] = "";
                    if (ClientID != null) dicClient[ClientID].CancelPendingRequests();
                }

                ClientID = "";
                Debug.WriteLine(ex.Message);
                if (ex.Message.IndexOf("(404) Not Found") > -1) return "(404) Not Found";
                Retries++;
                if (Retries >= 15) return "error";
            }
        }
    }

    private static void UpdateProxies()
    {
        bool done = false;
        while (!done)
        {
            try
            {
                string[]? lo_Proxies = null;

                string ProxyHTML = Download_v2("http://www.sslproxies.org");
                ProxyHTML = ProxyHTML.Substring(ProxyHTML.IndexOf("UTC.") + 4);
                ProxyHTML = ProxyHTML.Substring(0, ProxyHTML.IndexOf("</textarea>"));
                ProxyHTML = ProxyHTML.TrimStart('\n');
                ProxyHTML = ProxyHTML.TrimEnd('\n');
                lo_Proxies = ProxyHTML.Split('\n');

                if (Proxies == null)
                {
                    Proxies = new List<string>();
                }
                foreach (string newProx in lo_Proxies)
                {
                    if (!Proxies.Contains(newProx))
                    {
                        if (MakeNewClient(newProx))
                        {
                            Proxies.Add(newProx);
                            Console.WriteLine($"Http Client Init: {newProx}");
                        }
                    }
                }
                LastProxyUpdate = DateTime.Now;
                done = true;
            }
            catch (Exception)
            {
                Thread.Sleep(5000);
            }
        }
    }

    private static void UpdateUserAgents()
    {
        bool done = false;
        while (!done)
        {
            try
            {
                List<string>? UAsFromJSON = null;
                try
                {
                    string uaJSON = Download_v2("https://raw.githubusercontent.com/microlinkhq/top-user-agents/master/src/index.json");
                    UAsFromJSON = JsonSerializer.Deserialize<List<string>>(uaJSON);
                }
                catch (Exception)
                {
                    UAsFromJSON = new List<string> { "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/114.0" };
                }

                int Index = 1;
                lock (threadLock)
                {
                    dicUAs.Clear();
                    if (UAsFromJSON != null)
                    {
                        foreach (string UA in UAsFromJSON)
                        {
                            dicUAs[Index] = UA;
                            Index++;
                        }
                    }
                    else
                        dicUAs[Index] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36";
                }

                LastUAUpdate = DateTime.Now;
            }
            catch (Exception)
            {
                Thread.Sleep(5000);
            }
            finally
            {
                done = true;
            }
        }
    }

    public static string Download_v2(string URL)
    {
        try
        {
            Limit_API_Call_Rate();

            // Create a new request message with custom user agent
            using (var request = new HttpRequestMessage(HttpMethod.Get, URL))
            {
                request.Headers.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36");

                using (HttpResponseMessage IISresponse = client.SendAsync(request).Result)
                {
                    IISresponse.EnsureSuccessStatusCode(); // Throw if httpcode is an error
                    using (HttpContent content = IISresponse.Content)
                    {
                        string ReturnString = content.ReadAsStringAsync().Result;
                        return ReturnString;
                    }
                }
            }
        }
        catch (Exception)
        {
            return "error";
        }
    }


    //public static string Download(string URL)
    //{
    //    try
    //    {
    //        Limit_API_Call_Rate();
    //        using (HttpResponseMessage IISresponse = client.GetAsync(URL).Result)
    //        {
    //            IISresponse.EnsureSuccessStatusCode(); // Throw if httpcode is an error
    //            using (HttpContent content = IISresponse.Content)
    //            {
    //                string ReturnString = content.ReadAsStringAsync().Result;
    //                return ReturnString;
    //            }
    //        }
    //    }
    //    catch (Exception)
    //    {
    //        return "error";
    //    }
    //}

    private static void Limit_API_Call_Rate()
    {
        // Handle API Rate Limit
        int RequestPerMinute = 75;  // https://platform.openai.com/settings/organization/limits
        int MilliSecondsBetweenRequests = 60000 / RequestPerMinute;

        TimeSpan span = DateTime.Now - LastRequest;
        int MilliSeconds2Wait = MilliSecondsBetweenRequests - (int)span.TotalMilliseconds;
        if (MilliSeconds2Wait > 0)
            Thread.Sleep(MilliSeconds2Wait);
    }

    private static int GetRand(int Start, int End)
    {
        return random.Next(Start, End + 1);
    }

    private static object threadLock = new object();
}
