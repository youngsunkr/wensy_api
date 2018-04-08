using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;

using System.Web.Caching;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;


namespace SimpleWebService
{
    /// <summary>
    /// CheckDuplicatedClient의 요약 설명입니다.
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // ASP.NET AJAX를 사용하여 스크립트에서 이 웹 서비스를 호출하려면 다음 줄의 주석 처리를 제거합니다. 
    // [System.Web.Script.Services.ScriptService]
    public class CheckDuplicatedClient : System.Web.Services.WebService
    {

        const int MAX_CLIENT_AGENT = 2000;
        string strCacheID = "CLIENT_KEYS";

        public struct CLIENT_INFO
        {
            public string strProductKey;
            public string strMachineKey;
            public string strIP;
            public DateTime dtmLastUpdate;
        }

        public CLIENT_INFO[] ClientAgentInfo = new CLIENT_INFO[MAX_CLIENT_AGENT];
        
        [WebMethod]
        public bool IsValidClient(string strProductKey, string strMachineKey, string strIP)
        {            
            DataTable dtKeys = new DataTable();

            if (HttpRuntime.Cache[strCacheID] != null)
            {
                if (CheckAgentList(strProductKey, strMachineKey, strIP))
                    return true;
                else
                    return false;
            }
            else
            {
                // 배열 초기화.
                for (int i = 0; i < MAX_CLIENT_AGENT; i++)
                {
                    ClientAgentInfo[i].strMachineKey = "";
                    ClientAgentInfo[i].strProductKey = "";
                    ClientAgentInfo[i].strIP = "";
                }

                ClientAgentInfo[0].strMachineKey = strMachineKey;
                ClientAgentInfo[0].strProductKey = strProductKey;
                ClientAgentInfo[0].strIP = strIP;
                ClientAgentInfo[0].dtmLastUpdate = DateTime.Now;               

                HttpRuntime.Cache.Insert(strCacheID, ClientAgentInfo, null, DateTime.Now.AddSeconds(3600), Cache.NoSlidingExpiration);
            }

            return true;
        }

        [WebMethod]
        public bool DeleteRegistedAgent(string strProductKey, string strMachineKey, string strIP)
        {
            if (HttpRuntime.Cache[strCacheID] == null)
                return true;

            ClientAgentInfo = (CLIENT_INFO[])HttpRuntime.Cache[strCacheID];

            for (int i = 0; i < MAX_CLIENT_AGENT; i++)
            {
                if (strProductKey == ClientAgentInfo[i].strProductKey && strMachineKey == ClientAgentInfo[i].strMachineKey)
                {
                    ClientAgentInfo[i].strMachineKey = "";
                    ClientAgentInfo[i].strProductKey = "";
                    ClientAgentInfo[i].strIP = "";

                    HttpRuntime.Cache.Remove(strCacheID);
                    HttpRuntime.Cache.Insert(strCacheID, ClientAgentInfo, null, DateTime.Now.AddSeconds(3600), Cache.NoSlidingExpiration);

                    return true;
                }
            }

            return false;
        }

        bool CheckAgentList(string strProductKey, string strMachineKey, string strIP)
        {
            ClientAgentInfo = (CLIENT_INFO[])HttpRuntime.Cache[strCacheID];
            int iIndex = 0;

            for (int i = 0; i < MAX_CLIENT_AGENT; i++)
            {
                if (strProductKey == ClientAgentInfo[i].strProductKey)
                {
                    if (strMachineKey == ClientAgentInfo[i].strMachineKey)
                    {
                        return true;
                    }
                    else
                    {
                        TestLogFiles("ABNORMAL AGENT FOUND : " + strMachineKey + "," + strProductKey);
                        return false;
                    }
                }

                if (string.IsNullOrEmpty(ClientAgentInfo[i].strProductKey))
                    iIndex = i;
            }

            InsertAgentInfo(strProductKey, strMachineKey, strIP, iIndex);

            return true;
        }

        void InsertAgentInfo(string strProductKey, string strMachineKey, string strIP, int iIndex)
        {

            ClientAgentInfo[iIndex].strProductKey = strProductKey;
            ClientAgentInfo[iIndex].strMachineKey = strMachineKey;
            ClientAgentInfo[iIndex].strIP = strIP;
            ClientAgentInfo[iIndex].dtmLastUpdate = DateTime.Now;

            HttpRuntime.Cache.Remove(strCacheID);
            HttpRuntime.Cache.Insert(strCacheID, ClientAgentInfo, null, DateTime.Now.AddSeconds(3600), Cache.NoSlidingExpiration);
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

    }
}
