using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;
using System.Text;
using System.Data;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Data.SqlClient;
using Newtonsoft.Json;

namespace SimpleWebService
{
    /// <summary>
    /// AddRunningQueries의 요약 설명입니다.
    /// </summary>
    [WebService(Namespace = "http://sqlmvp.kr")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // ASP.NET AJAX를 사용하여 스크립트에서 이 웹 서비스를 호출하려면 다음 줄의 주석 처리를 제거합니다. 
    // [System.Web.Script.Services.ScriptService]
    public class AddRunningQueries : System.Web.Services.WebService
    {
        string strSPConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["SPConnectionString"].ConnectionString;

        [WebMethod]
        public int InsertRunningQueries(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            string jsonString = JsonConvert.SerializeObject(dtQueries);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {
                string strQuery = @"insert into tbSQLCurrentExecution_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - tbSQLCurrentExecution_JSON Error : " + ex.Message);
            }

            //try
            //{
            //    string strRecord = "";
                
            //    foreach (DataRow dr in dtQueries.Rows)
            //    {
            //        strRecord = " (N'" + strTimeIn_UTC + "', N'" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["Session_ID"].ToString() + "', N'" + dr["status"].ToString() + "', N'" + dr["db_name"].ToString()
            //            + "', N'" + dr["object_name"].ToString() + "', N'" + dr["command"].ToString() + "', N'" + dr["cpu_time"].ToString() + "', N'" + dr["total_elapsed_time"].ToString() + "', N'" + dr["logical_reads"].ToString()
            //            + "', N'" + dr["reads"].ToString() + "', N'" + dr["writes"].ToString() + "', N'" + dr["blocking_session_id"].ToString() + "', N'" + dr["wait_type"].ToString() + "', N'" + dr["wait_time"].ToString() + "', N'" + dr["wait_resource"].ToString()
            //            + "', N'" + dr["t_i_level"].ToString() + "', N'" + dr["row_count"].ToString() + "', N'" + dr["percent_complete"].ToString() + "', N'" + dr["full_query_text"].ToString().Replace("'", "`")
            //            + "', N'" + dr["user_objects_alloc_page_count"].ToString() + "', N'" + dr["user_objects_dealloc_page_count"].ToString() + "', N'" + dr["login_name"].ToString() + "', N'" + dr["original_login_name"].ToString() + "', N'" + dr["login_time"].ToString()
            //            + "', N'" + dr["host_name"].ToString() + "', N'" + dr["program_name"].ToString() + "', N'" + dr["client_interface_name"].ToString() + "', N'" + dr["nt_domain"].ToString() + "', N'" + dr["nt_user_name"].ToString() + "', N'" + dr["session_cpu_time"].ToString()
            //            + "', N'" + dr["session_memory_usage"].ToString() + "', N'" + dr["session_logical_reads"].ToString() + "', N'" + dr["session_physical_reads"].ToString() + "', N'" + dr["session_writes"].ToString() + "', N'" + dr["lock_timeout"].ToString() + "'),";

            //        strRecord = strRecord.Substring(0, strRecord.Length - 1);

            //        string strQuery = @"insert into tbSQLCurrentExecution (TimeIn_UTC, TimeIn, ServerNum, session_id, status, db_name, object_name, command, cpu_time, total_elapsed_time, logical_reads, reads, writes, blocking_session_id, wait_type, wait_time, wait_resource, t_i_level, row_count, percent_complete, full_query_text, user_objects_alloc_page_count, user_objects_dealloc_page_count, login_name, original_login_name, login_time, host_name, program_name, client_interface_name, nt_domain, nt_user_name, session_cpu_time, session_memory_usage ,session_logical_reads, session_physical_reads, session_writes, lock_timeout) values ";
            //        strQuery = strQuery + strRecord;

            //        RunQueryCommand(strQuery);
            //    }
            //}
            //catch (Exception ex)
            //{
            //    TestLogFiles("Running Query Insert Failed - CurrentExecution Error : " + ex.Message);
            //}

            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLJobAgentFailCheck(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            string jsonString = JsonConvert.SerializeObject(dtQueries);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {
                string strQuery = @"insert into tbSQLAgentFail_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - tbSQLAgentFail_JSON Error : " + ex.Message);
            }
            
            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLJobStatusCheck(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            string jsonString = JsonConvert.SerializeObject(dtQueries);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {
                string strQuery = @"insert into tbSQLAgentStatus_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - tbSQLAgentStatus_JSON Error : " + ex.Message);
            }
            
            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLLinkedCheck(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            try
            {
                string strRecord = "";
                
                foreach (DataRow dr in dtQueries.Rows)
                {
                    strRecord = " (N'" + strTimeIn_UTC + "', N'" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["Linked_Name"].ToString() + "'),";

                    strRecord = strRecord.Substring(0, strRecord.Length - 1);

                    string strQuery = @"insert into tbSQLLinkedCheck (TimeIn_UTC, TimeIn, ServerNum, LinkedName) values ";
                    strQuery = strQuery + strRecord;

                    RunQueryCommand(strQuery);
                }
            }
            catch (Exception ex)
            {
                TestLogFiles("Running Query Insert Failed - SQLLinkedCheck_Insert Error : " + ex.Message);
            }

            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLErrorlog(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            string jsonString = JsonConvert.SerializeObject(dtQueries);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {
                string strQuery = @"insert into tbSQLErrorlog_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - tbSQLErrorlog_JSON Error : " + ex.Message);
            }

            //try
            //{
            //    string strRecord = "";
                
            //    foreach (DataRow dr in dtQueries.Rows)
            //    {
            //        strRecord = " (N'" + strTimeIn_UTC + "', N'" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["LogDate"].ToString() + "', N'" + dr["ProcessInfo"].ToString() + "', N'" + dr["ErrorText"].ToString().Replace("'", "`") + "'),";

            //        strRecord = strRecord.Substring(0, strRecord.Length - 1);

            //        string strQuery = @"insert into tbSQLErrorlog (TimeIn_UTC,TimeIn,ServerNum,LogDate,ProcessInfo,ErrorText) values ";
            //        strQuery = strQuery + strRecord;

            //        RunQueryCommand(strQuery);
            //    }
            //}
            //catch (Exception ex)
            //{
            //    TestLogFiles("Running Query Insert Failed - SQLErrorlog_Insert Error : " + ex.Message);
            //}

            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLTableSize(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            string jsonString = JsonConvert.SerializeObject(dtQueries);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {
                string strQuery = @"insert into tbSQLTableSize_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - tbSQLTableSize_JSON Error : " + ex.Message);
            }

            //try
            //{
            //    string strRecord = "";
                
            //    foreach (DataRow dr in dtQueries.Rows)
            //    {
            //        strRecord = " (N'" + strTimeIn_UTC + "', N'" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["DatabaseName"].ToString() + "', N'" + dr["SchemaName"].ToString() + "', N'" + dr["TableName"].ToString() + "', N'" + dr["Row_Count"].ToString() + "', N'" + dr["Reserved_KB"].ToString() + "', N'" + dr["Data_KB"].ToString() + "', N'" + dr["IndexSize_KB"].ToString() + "', N'" + dr["Unused_KB"].ToString() + "'),";

            //        strRecord = strRecord.Substring(0, strRecord.Length - 1);

            //        string strQuery = @"insert into tbSQLTableSize (Timein_UTC,Timein,ServerNum,DatabaseName,SchemaName,TableName,Row_Count,Reserved_KB,Data_KB,IndexSize_KB,Unused_KB) values ";
            //        strQuery = strQuery + strRecord;

            //        RunQueryCommand(strQuery);
            //    }
            //}
            //catch (Exception ex)
            //{
            //    TestLogFiles("Running Query Insert Failed - SQLTableSize_Insert Error : " + ex.Message);
            //}

            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLBlock(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            string jsonString = JsonConvert.SerializeObject(dtQueries);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {
                string strQuery = @"insert into tbSQLBlock_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - tbSQLBlock_JSON Error : " + ex.Message);
            }

            //try
            //{
            //    string strRecord = "";
                
            //    foreach (DataRow dr in dtQueries.Rows)
            //    {
            //        strRecord = " (N'" + strTimeIn_UTC + "', N'" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["DatabaseName"].ToString() + "', N'" + dr["LAST_BATCH"].ToString() + "', N'" + dr["SPID"].ToString() + "', N'" + dr["Blocked_SPID"].ToString() + "', N'" + dr["WAITTIME"].ToString() + "', N'" + dr["LASTWAITTYPE"].ToString() + "', N'" + dr["STATUS"].ToString() + "', N'" + dr["LOGINAME"].ToString() + "', N'" + dr["Query_Text"].ToString() + "'),";

            //        strRecord = strRecord.Substring(0, strRecord.Length - 1);

            //        string strQuery = @"insert into tbSQLBlock (TimeIn_UTC, Timein, ServerNum, DatabaseName, LAST_BATCH, SPID, Blocked_SPID, WAITTIME, LASTWAITTYPE, STATUS, LOGINAME, Query_Text) values ";
            //        strQuery = strQuery + strRecord;

            //        RunQueryCommand(strQuery);
            //    }
            //}
            //catch (Exception ex)
            //{
            //    TestLogFiles("Running Query Insert Failed - SQLBlock_Insert Error : " + ex.Message);
            //}

            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLObjectCheck(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            string jsonString = JsonConvert.SerializeObject(dtQueries);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {
                string strQuery = @"insert into tbSQLObjectCheck_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - tbSQLObjectCheck_JSON Error : " + ex.Message);
            }

            //try
            //{
            //    string strRecord = "";
                
            //    foreach (DataRow dr in dtQueries.Rows)
            //    {
            //        strRecord = " (N'" + strTimeIn_UTC + "', N'" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["db_name"].ToString() + "', N'" + dr["name"].ToString() + "', N'" + dr["parent_object_name"].ToString() + "', N'" + dr["type_desc"].ToString() + "', N'" + dr["create_date"].ToString() + "', N'" + dr["modify_date"].ToString() + "'),";

            //        strRecord = strRecord.Substring(0, strRecord.Length - 1);

            //        string strQuery = @"insert into tbSQLObjectCheck (TimeIn_UTC, TimeIn, ServerNum, [db_name], name, parent_object_name, type_desc, create_date, modify_date) values ";
            //        strQuery = strQuery + strRecord;

            //        RunQueryCommand(strQuery);
            //    }
            //}
            //catch (Exception ex)
            //{
            //    TestLogFiles("Running Query Insert Failed - SQLObjectCheck_Insert Error : " + ex.Message);
            //}
            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLDatabase(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            string jsonString = JsonConvert.SerializeObject(dtQueries);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {

                string strQuery = @"insert into tbSQLDatabase_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);

            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - tbSQLDatabase_JSON Error : " + ex.Message);
            }

            //try
            //{
            //    string strRecord = "";
                
            //    foreach (DataRow dr in dtQueries.Rows)
            //    {
            //        strRecord = " (N'" + strTimeIn_UTC + "', N'" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["database_name"].ToString() + "', N'" + dr["create_date"].ToString() + "', N'" + dr["compatibility_level"].ToString() + "', N'" + dr["collation_name"].ToString() + "', N'" + dr["user_access_desc"].ToString() + "', N'" + dr["is_read_only"].ToString()
            //        + "', N'" + dr["is_auto_shrink_on"].ToString() + "', N'" + dr["state_desc"].ToString() + "', N'" + dr["is_in_standby"].ToString() + "', N'" + dr["snapshot_isolation_state_desc"].ToString() + "', N'" + dr["is_read_committed_snapshot_on"].ToString() + "', N'" + dr["recovery_model_desc"].ToString() + "', N'" + dr["page_verify_option_desc"].ToString()
            //        + "', N'" + dr["is_auto_create_stats_on"].ToString() + "', N'" + dr["is_auto_update_stats_on"].ToString() + "', N'" + dr["is_auto_update_stats_async_on"].ToString() + "', N'" + dr["is_fulltext_enabled"].ToString() + "', N'" + dr["is_trustworthy_on"].ToString() + "', N'" + dr["is_parameterization_forced"].ToString() + "', N'" + dr["is_db_chaining_on"].ToString()
            //        + "', N'" + dr["is_broker_enabled"].ToString() + "', N'" + dr["is_published"].ToString() + "', N'" + dr["is_subscribed"].ToString() + "', N'" + dr["is_merge_published"].ToString() + "', N'" + dr["is_distributor"].ToString() + "'),";

            //        strRecord = strRecord.Substring(0, strRecord.Length - 1);

            //        string strQuery = @"insert into tbSQLDatabase (TimeIn_UTC,TimeIn,ServerNum,database_name,create_date,compatibility_level,collation_name,user_access_desc,is_read_only,is_auto_shrink_on,state_desc,is_in_standby,snapshot_isolation_state_desc,is_read_committed_snapshot_on,recovery_model_desc,page_verify_option_desc,is_auto_create_stats_on,is_auto_update_stats_on,is_auto_update_stats_async_on,is_fulltext_enabled,is_trustworthy_on,is_parameterization_forced,is_db_chaining_on,is_broker_enabled,is_published,is_subscribed,is_merge_published,is_distributor) values ";
            //        strQuery = strQuery + strRecord;

            //        RunQueryCommand(strQuery);
            //    }
            //}
            //catch (Exception ex)
            //{
            //    TestLogFiles("Running Query Insert Failed - SQLDatabase_Insert Error : " + ex.Message);
            //}
            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLIndexDuplication(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            string jsonString = JsonConvert.SerializeObject(dtQueries);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {
                string strQuery = @"insert into tbSQLIndexDuplication_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - tbSQLIndexDuplication_JSON Error : " + ex.Message);
            }

            //try
            //{
            //    string strRecord = "";
                
            //    foreach (DataRow dr in dtQueries.Rows)
            //    {
            //        strRecord = " (N'" + strTimeIn_UTC + "', N'" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["DatabaseName"].ToString() + "', N'" + dr["ObjectName"].ToString() + "', N'" + dr["IndexName"].ToString() + "', N'" + dr["DuplicationIndexName"].ToString() + "', N'" + dr["is_primary_key"].ToString() + "', N'" + dr["is_unique"].ToString()
            //        + "', N'" + dr["type"].ToString() + "', N'" + dr["IndexColumns"].ToString() + "', N'" + dr["IncludedColumns"].ToString() + "'),";

            //        strRecord = strRecord.Substring(0, strRecord.Length - 1);

            //        string strQuery = @"insert into tbSQLIndexDuplication (TimeIn_UTC,TimeIn,ServerNum,DatabaseName,ObjectName,IndexName,DuplicationIndexName,is_primary_key,is_unique,type,IndexColumns,IncludedColumns) values ";
            //        strQuery = strQuery + strRecord;

            //        RunQueryCommand(strQuery);
            //    }
            //}
            //catch (Exception ex)
            //{
            //    TestLogFiles("Running Query Insert Failed - SQLIndexDuplication_Insert Error : " + ex.Message);
            //}

            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLServerInfo(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            try
            {
                string strRecord = "";
                string strQuery = "";
                
                foreach (DataRow dr in dtQueries.Rows)
                {
                    strRecord = " (N'" + strTimeIn_UTC + "', N'" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["MachineName"].ToString() + "', N'" + dr["ServerName"].ToString() + "', N'" + dr["Instance"].ToString() + "', N'" + dr["IsClustered"].ToString() + "', N'" + dr["ComputerNamePhysicalNetBIOS"].ToString() + "', N'" + dr["Edition"].ToString()
                    + "', N'" + dr["ProductLevel"].ToString() + "', N'" + dr["ProductVersion"].ToString() + "', N'" + dr["ProcessID"].ToString() + "', N'" + dr["Collation"].ToString() + "', N'" + dr["IsFullTextInstalled"].ToString() + "', N'" + dr["IsIntegratedSecurityOnly"].ToString() + "', N'" + dr["IsHadrEnabled"].ToString()
                    + "', N'" + dr["HadrManagerStatus"].ToString() + "', N'" + dr["IsXTPSupported"].ToString() + "'),";

                    strRecord = strRecord.Substring(0, strRecord.Length - 1);

                    strQuery = @"update tbSQLServerInfo set TimeIn_UTC = N'" + strTimeIn_UTC + "', TimeIn = N'" + strTimeIn + "', MachineName = N'" + dr["MachineName"].ToString() + "', ServerName = N'" + dr["ServerName"].ToString() + "', InstanceName = N'" + dr["Instance"].ToString() + "', IsClustered = N'" + dr["IsClustered"].ToString()
                    + "', ComputerNamePhysicalNetBIOS = N'" + dr["ComputerNamePhysicalNetBIOS"].ToString() + "', Edition = N'" + dr["Edition"].ToString() + "', ProductLevel = N'" + dr["ProductLevel"].ToString() + "', ProductVersion = N'" + dr["ProductVersion"].ToString() + "', ProcessID = N'" + dr["ProcessID"].ToString() + "', Collation = N'" + dr["Collation"].ToString()
                    + "', IsFullTextInstalled = N'" + dr["IsFullTextInstalled"].ToString() + "', IsIntegratedSecurityOnly = N'" + dr["IsIntegratedSecurityOnly"].ToString() + "', IsHadrEnabled = N'" + dr["IsHadrEnabled"].ToString() + "', HadrManagerStatus = N'" + dr["HadrManagerStatus"].ToString() + "', IsXTPSupported = N'" + dr["IsXTPSupported"].ToString() + "' where ServerNum = " + iServerNumber + "if @@ROWCOUNT = 0 "
                    + "insert into tbSQLServerInfo (TimeIn_UTC,TimeIN,ServerNum,MachineName,ServerName,InstanceName,IsClustered,ComputerNamePhysicalNetBIOS,Edition,ProductLevel,ProductVersion,ProcessID,Collation,IsFullTextInstalled,IsIntegratedSecurityOnly,IsHadrEnabled,HadrManagerStatus,IsXTPSupported) values ";
                    strQuery = strQuery + strRecord;

                    RunQueryCommand(strQuery);
                }
            }
            catch (Exception ex)
            {
                TestLogFiles("Running Query Insert Failed - SQLServerInfo_Insert Error : " + ex.Message);
            }
            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLServiceStatus(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            string jsonString = JsonConvert.SerializeObject(dtQueries);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {
                string strQuery = @"insert into tbSQLServiceStatus_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - tbSQLServiceStatus_JSON Error : " + ex.Message);
            }

            //try
            //{
            //    string strRecord = "";

            //    foreach (DataRow dr in dtQueries.Rows)
            //    {
            //        strRecord = " (N'" + strTimeIn_UTC + "', N'" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["servicename"].ToString() + "', N'" + dr["process_id"].ToString() + "', N'" + dr["startup_type_desc"].ToString() + "', N'" + dr["status_desc"].ToString()
            //        + "', N'" + dr["last_startup_time"].ToString() + "', N'" + dr["service_account"].ToString() + "', N'" + dr["is_clustered"].ToString() + "', N'" + dr["cluster_nodename"].ToString() + "', N'" + dr["filename"].ToString() + "'),";

            //        strRecord = strRecord.Substring(0, strRecord.Length - 1);

            //        string strQuery = @"insert into tbSQLServiceStatus (TimeIN_UTC, TimeIn, ServerNum, servicename, process_id, startup_type_desc, status_desc, last_startup_time, service_account, is_clustered, cluster_nodename, [filename]) values ";
            //        strQuery = strQuery + strRecord;

            //        RunQueryCommand(strQuery);
            //    }
            //}
            //catch (Exception ex)
            //{
            //    TestLogFiles("Running Query Insert Failed - SQLServiceStatus_Insert Error : " + ex.Message);
            //}

            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLConfiguration(Byte[] bytearr, int iServerNumber, string strTimeIn)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            try
            {
                string strRecord = "";
                string strQuery = "";
                
                foreach (DataRow dr in dtQueries.Rows)
                {
                    strRecord = " (" + iServerNumber.ToString() + ", N'" + dr["Name"].ToString() + "', N'" + dr["Value"].ToString() + "', N'" + dr["Minimum"].ToString() + "', N'" + dr["Maximum"].ToString() + "', N'" + dr["Value_in_use"].ToString()
                    + "', N'" + dr["is_dynamic"].ToString() + "', N'" + dr["is_advanced"].ToString() + "'),";

                    strRecord = strRecord.Substring(0, strRecord.Length - 1);

                    strQuery = "update tbSQLConfiguration_Server set ServerNum = " + iServerNumber + ", Name = N'" + dr["Name"].ToString() + "', Value = N'" + dr["Value"].ToString() + "', Minimum = N'" + dr["Minimum"].ToString() + "', Maximum = N'" + dr["Maximum"].ToString() + "', Value_in_use = N'" + dr["Value_in_use"].ToString() + "', is_dynamic = N'" + dr["is_dynamic"].ToString() + "', is_advanced = N'" + dr["is_advanced"].ToString() + "' where ServerNum = " + iServerNumber + " and Name = N'" + dr["Name"].ToString() + "' if @@ROWCOUNT = 0 "
                    + "insert into tbSQLConfiguration_Server (ServerNum, Name, Value, Minimum, Maximum, Value_in_use, is_dynamic, is_advanced) values ";
                    strQuery = strQuery + strRecord;

                    RunQueryCommand(strQuery);
                }
            }
            catch (Exception ex)
            {
                TestLogFiles("Running Query Insert Failed - SQLConfiguration_Insert Error : " + ex.Message);
            }

            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLDataBaseFileSize(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            string jsonString = JsonConvert.SerializeObject(dtQueries);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {
                string strQuery = @"insert into tbSQLDataBaseFileSize_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - tbSQLDataBaseFileSize_JSON Error : " + ex.Message);
            }

            //try
            //{
            //    string strRecord = "";
                
            //    foreach (DataRow dr in dtQueries.Rows)
            //    {
            //        strRecord = " (N'" + strTimeIn_UTC + "', N'" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["DatabaseName"].ToString() + "', N'" + dr["Total_Databases_Size_MB"].ToString() + "', N'" + dr["Datafile_Size_MB"].ToString() + "', N'" + dr["Reserved_MB"].ToString() + "', N'" + dr["Reserved_Percent"].ToString() + "', N'" + dr["Unallocated_Space_MB"].ToString()
            //        + "', N'" + dr["Unallocated_Percent"].ToString() + "', N'" + dr["Data_MB"].ToString() + "', N'" + dr["Data_Percent"].ToString() + "', N'" + dr["Index_MB"].ToString() + "', N'" + dr["Index_Percent"].ToString() + "', N'" + dr["Unused_MB"].ToString() + "', N'" + dr["Unused_Percent"].ToString() + "', N'" + dr["Transaction_Log_Size"].ToString()
            //        + "', N'" + dr["Log_Size_MB"].ToString() + "', N'" + dr["Log_used_Size_MB"].ToString() + "', N'" + dr["Log_Used_Size_Percent"].ToString() + "', N'" + dr["Log_Unused_Size_MB"].ToString() + "', N'" + dr["Log_UnUsed_Size_Percent"].ToString() + "', N'" + dr["Avg_vlf_Size"].ToString() + "', N'" + dr["Total_vlf_cnt"].ToString() + "', N'" + dr["Active_vlf_cnt"].ToString() + "'),";

            //        strRecord = strRecord.Substring(0, strRecord.Length - 1);

            //        string strQuery = @"insert into tbSQLDataBaseFileSize (TimeIn_UTC,TimeIn,ServerNum,DatabaseName,Total_Databases_Size_MB,Datafile_Size_MB,Reserved_MB,Reserved_Percent,Unallocated_Space_MB,Unallocated_Percent,Data_MB,Data_Percent,Index_MB,Index_Percent,Unused_MB,Unused_Percent,Transaction_Log_Size,Log_Size_MB,Log_Used_Size_MB,Log_Used_Size_Percent,Log_Unused_Size_MB,Log_UnUsed_Size_Percent,Avg_vlf_Size,Total_vlf_cnt,Active_vlf_cnt) values ";
            //        strQuery = strQuery + strRecord;

            //        RunQueryCommand(strQuery);
            //    }
            //}
            //catch (Exception ex)
            //{
            //    TestLogFiles("Running Query Insert Failed - SQLDataBaseFileSize_Insert Error : " + ex.Message);
            //}

            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLSession(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            string jsonString = JsonConvert.SerializeObject(dtQueries);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {
                string strQuery = @"insert into tbSQLSession_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - tbSQLSession_JSON Error : " + ex.Message);
            }

            //try
            //{
            //    string strRecord = "";

            //    foreach (DataRow dr in dtQueries.Rows)
            //    {
            //        strRecord = " (N'" + strTimeIn_UTC + "', N'" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["Login_Name"].ToString() + "', N'" + dr["Host_Name"].ToString() + "', N'" + dr["Client_Net_Address"].ToString() + "', N'" + dr["TotalSession"].ToString() + "', N'" + dr["ActiveSession"].ToString() + "'),";

            //        strRecord = strRecord.Substring(0, strRecord.Length - 1);

            //        string strQuery = @"insert into tbSQLSession (TimeIn_UTC, TimeIn, ServerNum, Login_Name, Host_Name, Client_Net_Address, TotalSession, ActiveSession) values ";
            //        strQuery = strQuery + strRecord;

            //        RunQueryCommand(strQuery);
            //    }
            //}
            //catch (Exception ex)
            //{
            //    TestLogFiles("Running Query Insert Failed - SQLSession_Insert Error : " + ex.Message);
            //}

            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLIndexFlagment(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            string jsonString = JsonConvert.SerializeObject(dtQueries);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {
                string strQuery = @"insert into tbSQLIndexFlagment_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - tbSQLIndexFlagment_JSON Error : " + ex.Message);
            }

            //try
            //{
            //    string strRecord = "";
                
            //    foreach (DataRow dr in dtQueries.Rows)
            //    {
            //        strRecord = " (N'" + strTimeIn_UTC + "', N'" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["db_name"].ToString() + "', N'" + dr["object_name"].ToString() + "', N'" + dr["index_name"].ToString() + "', N'" + dr["index_type"].ToString() + "', N'" + dr["alloc_unit_type"].ToString()
            //        + "', N'" + dr["index_depth"].ToString() + "', N'" + dr["index_level"].ToString() + "', N'" + dr["avg_frag_percent"].ToString() + "', N'" + dr["fragment_count"].ToString() + "', N'" + dr["avg_frag_size_in_page"].ToString() + "', N'" + dr["page_count"].ToString() + "'),";

            //        strRecord = strRecord.Substring(0, strRecord.Length - 1);

            //        string strQuery = @"insert into tbSQLIndexFlagment (TimeIn_UTC, TimeIn, ServerNum, [db_name], [object_name], index_name, index_type, alloc_unit_type, index_depth, index_level, avg_frag_percent, fragment_count, avg_frag_size_in_page, page_count) values ";
            //        strQuery = strQuery + strRecord;

            //        RunQueryCommand(strQuery);
            //    }
            //}
            //catch (Exception ex)
            //{
            //    TestLogFiles("Running Query Insert Failed - SQLIndexFlagment_Insert Error : " + ex.Message);
            //}

            return dtQueries.Rows.Count;
        }

        [WebMethod]
        public int SQLAgentErrorlog(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtQueries = new DataTable();

            if (bytearr == null)
                return 0;

            dtQueries = BytesToDataTable(bytearr);

            if (dtQueries == null)
                return 0;

            string jsonString = JsonConvert.SerializeObject(dtQueries);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {
                string strQuery = @"insert into tbSQLAgentErrorlog_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - tbSQLAgentErrorlog_JSON Error : " + ex.Message);
            }
            
            return dtQueries.Rows.Count;
        }

        

        void RunQueryCommand(string strQuery)
        {

            using (SqlConnection sqlConn = new SqlConnection(strSPConnectionString))
            {
                try
                {
                    SqlCommand sqlComm = new SqlCommand();
                    sqlComm = sqlConn.CreateCommand();

                    sqlComm.CommandText = strQuery;

                    sqlConn.Open();
                    sqlComm.ExecuteNonQuery();

                    sqlConn.Close();
                }
                catch (Exception ex)
                {
                    TestLogFiles("Running Query Insert RunQueryCommand Error - " + ex.Message + " : " + strQuery);
                }
            }
        }
                
        void TestLogFiles(string strLogMsg)
        {

            string strPath = System.Configuration.ConfigurationManager.AppSettings["LogPath"].ToString();
            
            try
            {
                using (StreamWriter sw = new StreamWriter(strPath, true))
                {
                    sw.WriteLine(DateTime.Now.ToString() + ", " + strLogMsg);
                    sw.Close();
                }
            }
            catch (Exception ex)
            {
                return;
            }
        }

        private DataTable BytesToDataTable(Byte[] byteArray)
        {
            DataTable dtResult = new DataTable();

            BinaryFormatter bformatter = new BinaryFormatter();
            MemoryStream stream = new MemoryStream();

            try
            {
                if (byteArray != null)
                {
                    bformatter = new BinaryFormatter();
                    stream = new MemoryStream(byteArray);
                    dtResult = (DataTable)bformatter.Deserialize(stream);
                }
                else
                    dtResult = null;
            }
            catch (Exception ex)
            {                
                return null;
            }

            return dtResult;
        }

        private Byte[] DataTableToBytes(DataTable dtInput)
        {
            BinaryFormatter bformatter = new BinaryFormatter();
            MemoryStream stream = new MemoryStream();

            try
            {
                if (dtInput != null)
                {
                    bformatter.Serialize(stream, dtInput);
                    byte[] b = stream.ToArray();
                    stream.Close();
                    return b;
                }
                return null;
            }
            catch (Exception ex)
            {                
                return null;
            }

        }

    }
}
