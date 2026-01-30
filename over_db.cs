using CapstoneStockAnlyzer;
using Microsoft.Data.SqlClient;
using System.Collections;
using System.Data;
using System.Net.Mail;

public class over_db
{
    private static SqlConnection? lo_Connection;
    private const int CommandTimeOutSeconds = 1200;

    private static Dictionary<string, DateTime> _lastErrorEmailTimes = new Dictionary<string, DateTime>();
    private const int MIN_HOURS_BETWEEN_ERROR_EMAILS = 1;

    public static bool BulkInsertDataTable(ref DataTable dt, string TableName)
    {
        SqlConnection? connection = null;
        try
        {
            // Create new connection every time
            connection = new SqlConnection(ConnString);
            connection.Open();

            int Retry = 10;
            while (Retry >= 0)
            {
                try
                {
                    // Instantiate SqlBulkCopy with default options,
                    // supplying an open SqlConnection to the database
                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, null))
                    {
                        // The table I'm loading the data to
                        bulkCopy.DestinationTableName = TableName;
                        // How many records to send to the database in one go (all of them)
                        bulkCopy.BatchSize = dt.Rows.Count;
                        // Column Mappings

                        // Load the data to the database
                        bulkCopy.WriteToServer(dt);
                        // Close up          
                        bulkCopy.Close();
                    }

                    if (connection != null)
                    {
                        connection.Close();
                        connection.Dispose();
                    }

                    return true;
                }
                catch (Exception)
                {
                    if (Retry >= 1)
                    {
                        System.Threading.Thread.Sleep(1337);
                        Retry -= 1;
                    }
                    else
                    {
                        if (connection != null)
                        {
                            connection.Close();
                            connection.Dispose();
                        }
                        Retry = -1;
                    }
                }
            }
            return false;
        }
        catch (Exception ex)
        {
            // Ensure connection is properly disposed on error
            try
            {
                if (connection != null)
                {
                    if (connection.State == ConnectionState.Open)
                        connection.Close();
                    connection.Dispose();
                }
            }
            catch { } // Ignore errors when disposing connection

            // Optional: Add error logging similar to your other method
            SendErrorEmail("BulkInsertError", "db.algoarmada.com API: Critical error in BulkInsertDataTable", ex);
            return false;
        }
    }

    public static DataTable RemoveDuplicateRows(DataTable dTable, string colName)
    {
        Hashtable hTable = new Hashtable();
        ArrayList duplicateList = new ArrayList();

        //Add list of all the unique item value to hashtable, which stores combination of key, value pair.
        //And add duplicate item value in arraylist.
        foreach (DataRow drow in dTable.Rows)
        {
            if (hTable.Contains(drow[colName]))
                duplicateList.Add(drow);
            else
                hTable.Add(drow[colName], string.Empty);
        }

        //Removing a list of duplicate items from datatable.
        foreach (DataRow dRow in duplicateList)
            dTable.Rows.Remove(dRow);

        //Datatable which contains unique records will be return as output.
        return dTable;
    }

    static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    public static long ConvertToTimestamp(DateTime value)
    {
        TimeSpan elapsedTime = value - Epoch;
        return (long)elapsedTime.TotalSeconds;
    }

    public static DateTime UnixTimeStampToDateTime(long unixTimeStamp)
    {
        // Unix timestamp is seconds past epoch
        System.DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
        dtDateTime = dtDateTime.AddSeconds(unixTimeStamp).ToLocalTime();

        return dtDateTime;
    }

    public static bool FillDataTable_ViaSql(ref DataTable? ReturnTable, string SqlStr)
    {
        try
        {
            SqlCommand SqlCmd = new SqlCommand(SqlStr);
            return FillDataTable_ViaCmd(ref ReturnTable, ref SqlCmd);
        }
        catch (Exception ex)
        {
            SendErrorEmail("FillDataTableViaSqlError", "db.algoarmada.com API: Critical error in FillDataTable_ViaSql", ex);
            return false;
        }
    }

    public static bool FillDataTable_ViaCmd(ref DataTable? ReturnTable, ref SqlCommand SqlCmd, bool useAlt = false)
    {
        try
        {
            SqlDataAdapter? lo_Ada = new SqlDataAdapter();
            DataTable Return_DataTable = new DataTable();

            if (!OpenConnection())
            {
                throw new Exception("Failed to open database connection");
            }

            SqlCmd.Connection = lo_Connection;
            SqlCmd.CommandTimeout = CommandTimeOutSeconds;

            int Retry = 2;
            while (Retry >= 0)
            {
                try
                {
                    if (lo_Ada != null && lo_Connection != null)
                    {
                        lo_Ada.SelectCommand = SqlCmd;
                        lo_Ada.Fill(Return_DataTable);
                        lo_Ada.Dispose();
                        lo_Ada = null;
                        lo_Connection.Close();
                        ReturnTable = Return_DataTable;
                        return true;
                    }
                }
                catch (Exception ex)
                {
                    if (Retry >= 1 && ex.Message.Contains("deadlock victim"))
                    {
                        System.Threading.Thread.Sleep(3337);
                        Retry -= 1;
                    }
                    else if (Retry >= 1 && (ex.Message.Contains("INSERT EXEC failed ") || ex.Message.Contains("Schema changed ")))
                    {
                        System.Threading.Thread.Sleep(3337);
                        Retry -= 1;
                    }
                    else
                    {
                        if (lo_Connection != null) lo_Connection.Close();
                        Retry = -1;
                        throw;
                    }
                }
            }
            return false;
        }
        catch (Exception ex)
        {
            SendErrorEmail("FillDataTableViaCmdError", "db.algoarmada.com API: Critical error in FillDataTable_ViaCmd", ex);
            return false;
        }
    }

    public static bool ExecSqlString(string SqlString)
    {
        try
        {
            SqlCommand SqlCmd = new SqlCommand(SqlString);
            return ExecSqlCommand(ref SqlCmd);
        }
        catch (Exception ex)
        {
            SendErrorEmail("ExecSqlStringError", "db.algoarmada.com API: Critical error in ExecSqlString", ex);
            return false;
        }
    }

    public static bool ExecSqlCommand(ref SqlCommand SqlCmd)
    {
        SqlConnection? connection = null;
        try
        {
            // Create new connection every time
            connection = new SqlConnection(ConnString);
            connection.Open();

            SqlCmd.Connection = connection;
            SqlCmd.CommandTimeout = CommandTimeOutSeconds;

            int Retry = 2;
            while (Retry >= 0)
            {
                try
                {
                    SqlCmd.ExecuteNonQuery();
                    connection.Close();
                    connection.Dispose();
                    return true;
                }
                catch (Exception ex)
                {
                    if (Retry >= 1 && ex.Message.Contains("deadlock victim"))
                    {
                        System.Threading.Thread.Sleep(3337);
                        Retry -= 1;
                    }
                    else if (Retry >= 1 && (ex.Message.Contains("INSERT EXEC failed ") || ex.Message.Contains("Schema changed ")))
                    {
                        System.Threading.Thread.Sleep(3337);
                        Retry -= 1;
                    }
                    else
                    {
                        if (connection != null)
                        {
                            connection.Close();
                            connection.Dispose();
                        }
                        Retry = -1;
                        throw;
                    }
                }
            }
            return false;
        }
        catch (Exception ex)
        {
            // Ensure connection is properly disposed on error
            try
            {
                if (connection != null)
                {
                    if (connection.State == ConnectionState.Open)
                        connection.Close();
                    connection.Dispose();
                }
            }
            catch { } // Ignore errors when disposing connection

            SendErrorEmail("ExecSqlCommandError", "db.algoarmada.com API: Critical error in ExecSqlCommand", ex);
            return false;
        }
    }

    public static object RetrieveSingleValue(string Sql)
    {
        SqlCommand objCom = new SqlCommand();
        object ld_Result;
        if (!OpenConnection())
            return 0;

        try
        {
            objCom.Connection = lo_Connection;
            objCom.CommandTimeout = CommandTimeOutSeconds;
            objCom.CommandText = Sql;
            ld_Result = objCom.ExecuteScalar();

            return ld_Result;
        }
        catch (Exception)
        {
            return 0;
        }
        finally
        {
            if (lo_Connection != null) lo_Connection.Close();
        }
    }

    public static string ConnString = "Data Source=db.capstonestockanalyzer.com;Initial Catalog=StockAnalyzer;User Id=jk2026;Password=L$U!p8r55YYvh5$Z4!k2kT9$P!bdCNpDBzO!hlr;TrustServerCertificate=True;";

    public static bool OpenConnection()
    {
        try
        {
            int Retry = 2;
            while (Retry >= 0)
            {
                try
                {
                    if (lo_Connection == null || lo_Connection.ConnectionString == "")
                    {
                        lo_Connection = new SqlConnection();
                        lo_Connection.ConnectionString = ConnString;
                    }

                    if (lo_Connection.State == ConnectionState.Closed)
                        lo_Connection.Open();

                    return true;
                }
                catch (Exception ex)
                {
                    lo_Connection?.Dispose();
                    lo_Connection = null;
                    if (Retry >= 1 && (ex.Message.Contains("Data Provider error 6") || ex.Message.Contains("An existing connection was forcibly closed") || ex.Message.Contains("The specified network name is no longer available") || ex.Message.Contains("The semaphore timeout period has expired") || ex.Message.Contains("The timeout period elapsed prior to completion of the operation")))
                    {
                        System.Threading.Thread.Sleep(1337);
                        Retry -= 1;
                    }
                    else if (Retry >= 1 && (ex.Message.Contains("The server was not found or was not accessible") || ex.Message.Contains("Could not open a connection to SQL Server")))
                    {
                        System.Threading.Thread.Sleep(91337);
                        Retry -= 1;
                    }
                    else
                    {
                        Retry = -1;
                        throw;
                    }
                }
            }
            return false;
        }
        catch (Exception ex)
        {
            SendErrorEmail("OpenConnectionError", "db.algoarmada.com API: Critical error in OpenConnection", ex);
            return false;
        }
    }

    public static void SendErrorEmail(string errorType, string subject, Exception ex)
    {
        if (_lastErrorEmailTimes.TryGetValue(errorType, out DateTime lastSent))
        {
            if ((Program.GetCentralTimeNow() - lastSent).TotalHours < MIN_HOURS_BETWEEN_ERROR_EMAILS)
            {
                return;
            }
        }

        string message = $"Error occurred at {Program.GetCentralTimeNow().ToString("yyyy-MM-dd HH:mm:ss")}\n\n" +
                       $"Message: {ex.Message}\n\n" +
                       $"Stack Trace: {ex.StackTrace}\n\n" +
                       $"Inner Exception: {(ex.InnerException != null ? ex.InnerException.Message : "None")}";

        if (SendEmail("jshipp@algoarmada.com", subject, message))
        {
            _lastErrorEmailTimes[errorType] = Program.GetCentralTimeNow();
        }
    }

    public static bool SendEmail(string EmailAddress, string Subject, string Message)
    {
        try
        {
            MailMessage Mail = new MailMessage();
            Mail.IsBodyHtml = true;
            SmtpClient SMTP = new SmtpClient("smtp.gmail.com", 587);

            Mail.Subject = Subject;
            Mail.From = new MailAddress("noreply@algoarmada.com", "AlgoArmada.com");
            Mail.Sender = Mail.From;
            SMTP.Credentials = new System.Net.NetworkCredential("jshipp@algoarmada.com", "kzqlkcbrvohndpli");

            Mail.To.Add(EmailAddress);
            Mail.Bcc.Add("jshipp@algoarmada.com");
            Mail.Body = "<!doctype html> <html> <head> <meta name='viewport' content='width=device-width' /> <meta http-equiv='Content-Type' content='text/html; charset=UTF-8' /> <title>Simple Transactional Email</title> <style> /* ------------------------------------- INLINED WITH htmlemail.io/inline ------------------------------------- */ /* ------------------------------------- RESPONSIVE AND MOBILE FRIENDLY STYLES ------------------------------------- */ @media only screen and (max-width: 620px) { table[class=body] h1 { font-size: 28px !important; margin-bottom: 10px !important; } table[class=body] p, table[class=body] ul, table[class=body] ol, table[class=body] td, table[class=body] span, table[class=body] a { font-size: 16px !important; } table[class=body] .wrapper, table[class=body] .article { padding: 10px !important; } table[class=body] .content { padding: 0 !important; } table[class=body] .container { padding: 0 !important; width: 100% !important; } table[class=body] .main { border-left-width: 0 !important; border-radius: 0 !important; border-right-width: 0 !important; } table[class=body] .btn table { width: 100% !important; } table[class=body] .btn a { width: 100% !important; } table[class=body] .img-responsive { height: auto !important; max-width: 100% !important; width: auto !important; } } /* ------------------------------------- PRESERVE THESE STYLES IN THE HEAD ------------------------------------- */ @media all { .ExternalClass { width: 100%; } .ExternalClass, .ExternalClass p, .ExternalClass span, .ExternalClass font, .ExternalClass td, .ExternalClass div { line-height: 100%; } .apple-link a { color: inherit !important; font-family: inherit !important; font-size: inherit !important; font-weight: inherit !important; line-height: inherit !important; text-decoration: none !important; } #MessageViewBody a { color: inherit; text-decoration: none; font-size: inherit; font-family: inherit; font-weight: inherit; line-height: inherit; } .btn-primary table td:hover { background-color: #34495e !important; } .btn-primary a:hover { background-color: #34495e !important; border-color: #34495e !important; } } </style> </head> <body class='' style='background-color: #f6f6f6; font-family: sans-serif; -webkit-font-smoothing: antialiased; font-size: 14px; line-height: 1.4; margin: 0; padding: 0; -ms-text-size-adjust: 100%; -webkit-text-size-adjust: 100%;'> <span class='preheader' style='color: transparent; display: none; height: 0; max-height: 0; max-width: 0; opacity: 0; overflow: hidden; visibility: hidden; width: 0;'>" + Subject + "</span> <table border='0' cellpadding='0' cellspacing='0' class='body' style='border-collapse: separate; width: 100%; background-color: #f6f6f6;'> <tr> <td style='font-family: sans-serif; font-size: 14px; vertical-align: top;'> </td> <td class='container' style='font-family: sans-serif; font-size: 14px; vertical-align: top; display: block; Margin: 0 auto; max-width: 580px; padding: 10px; width: 580px;'> <div class='content' style='box-sizing: border-box; display: block; Margin: 0 auto; max-width: 580px; padding: 10px;'> <!-- START CENTERED WHITE CONTAINER --> <table class='main' style='border-collapse: separate; width: 100%; background: #ffffff; border-radius: 3px;'> <!-- START MAIN CONTENT AREA --> <tr> <td class='wrapper' style='font-family: sans-serif; font-size: 14px; vertical-align: top; box-sizing: border-box; padding: 20px;'> <table border='0' cellpadding='0' cellspacing='0' style='border-collapse: separate; width: 100%;'> <tr> <td style='font-family: sans-serif; font-size: 14px; vertical-align: top;'> <div style='display: flex;align-items: center;'><h4>Algo Armada " + Subject + "</h4> </div> <p style='font-family: sans-serif; font-size: 14px; font-weight: normal; margin: 0; Margin-bottom: 15px;'>" + Message + "</p> <p style='font-family: sans-serif; font-size: 14px; font-weight: normal; margin: 0; Margin-bottom: 15px;'><br>Thanks,<br>The AlgoArmada Development Team</p> <table border='0' cellpadding='0' cellspacing='0' class='btn btn-primary' style='border-collapse: separate; width: 100%; box-sizing: border-box;'> <tbody> <tr> <td align='left' style='font-family: sans-serif; font-size: 14px; vertical-align: top; padding-bottom: 15px;'> <table border='0' cellpadding='0' cellspacing='0' style='border-collapse: separate; width: auto;'> <tbody> <tr> <td style='font-family: sans-serif; font-size: 14px; vertical-align: top; background-color: #3498db; border-radius: 5px; text-align: center;'> <a href='http://algoarmada.com' target='_blank' style='display: inline-block; color: #ffffff; background-color: #3498db; border: solid 1px #3498db; border-radius: 5px; box-sizing: border-box; cursor: pointer; text-decoration: none; font-size: 14px; font-weight: bold; margin: 0; padding: 12px 25px; text-transform: capitalize; border-color: #3498db;'>Visit AlgoArmada.com</a> </td> </tr> </tbody> </table> </td> </tr> </tbody> </table> </td> </tr> </table> </td> </tr> <!-- END MAIN CONTENT AREA --> </table> <!-- END CENTERED WHITE CONTAINER --> </div> </td> <td style='font-family: sans-serif; font-size: 14px; vertical-align: top;'> </td> </tr> </table> </body> </html>";

            SMTP.EnableSsl = true;
            SMTP.Send(Mail);
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
