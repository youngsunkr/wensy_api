using System;
using System.Data;
using System.Configuration;
using System.Data.SqlClient;

namespace DB
{
    /// <summary>
    /// DB_Base의 요약 설명입니다.
    /// </summary>

    public abstract class DB_Base
    {
        protected SqlConnection con;
        protected SqlCommand cmd;

        public int nReturn;
        public DataSet dsReturn;

        public DB_Base()
        {
            con = new SqlConnection();
            cmd = new SqlCommand();
        }

        protected virtual void Open(string dbName, string spName)
        {
            Open(dbName);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = spName;
        }
        protected virtual void Open(string dbName)
        {
            if (con.State != ConnectionState.Open)
            {
                con.ConnectionString = ConfigurationManager.ConnectionStrings[dbName].ConnectionString;
                con.Open();
            }
            cmd.Connection = con;
            cmd.CommandTimeout = 120;
        }
        protected virtual void SetCmdCon(string dbName)
        {
            con.ConnectionString = ConfigurationManager.ConnectionStrings[dbName].ConnectionString;
            cmd.Connection = con;
        }
        protected virtual void SetProcedure(string spnName)
        {
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = spnName;
        }
        protected virtual void SetQuery(string sql)
        {
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = sql;
        }
        protected virtual void Close()
        {
            if (con.State == ConnectionState.Open)
            {
                con.Close();
            }
        }

        protected virtual void Execute()
        {
            cmd.ExecuteNonQuery();
            GetFrameParameter();
        }

        protected virtual object ExecuteScalar()
        {
            object obj = cmd.ExecuteScalar();
            GetFrameParameter();
            return obj;
        }

        protected virtual DataSet ExecuteDataSet()
        {
            SqlDataAdapter ad = new SqlDataAdapter(cmd);
            DataSet ds = new DataSet();
            ad.Fill(ds);
            //GetFrameParameter();
            nReturn = GetResult(ds);
            return ds;
        }
        protected virtual DataSet ExecuteDataSet(string strParam)
        {
            SqlDataAdapter ad = new SqlDataAdapter(cmd);
            DataSet ds = new DataSet();
            ad.Fill(ds);
            nReturn = Convert.ToInt32(GetParameter(strParam));
            return ds;
        }
        public int GetResult(DataSet ds)
        {
            if (ds.Tables[0].Columns.Contains("Result"))
                return Convert.ToInt32(ds.Tables[0].Rows[0]["Result"]);
            else
                return 1;
        }
        protected virtual SqlDataReader ExecuteDataReader()
        {
            SqlDataReader reader = cmd.ExecuteReader();
            GetFrameParameter();
            return reader;
        }

        protected virtual void InitFrameParameter()
        {
            ClareParameter();
            AddParameter("RETURN_VALUE", SqlDbType.Int, ParameterDirection.ReturnValue, 4, DBNull.Value);


        }
        protected virtual void GetFrameParameter()
        {
            nReturn = Convert.ToInt32(GetParameter("Result"));
        }
        protected virtual void AddParameter(string paramName, SqlDbType type, ParameterDirection direction, int size, object value)
        {
            if (value == null)
                value = DBNull.Value;

            cmd.Parameters.Add(paramName, type, size);
            cmd.Parameters[paramName].Value = value;
            cmd.Parameters[paramName].Direction = direction;
        }
        protected virtual void AddParameter(string paramName, SqlDbType type, int size, object value)
        {
            AddParameter(paramName, type, ParameterDirection.Input, size, value);
        }
        protected virtual void AddParameter(string paramName, object value)
        {
            AddParameter(paramName, ParameterDirection.Input, value);
        }
        protected virtual void AddParameter(string paramName, ParameterDirection direction, object value)
        {
            if (value == null)
                value = DBNull.Value;

            cmd.Parameters.AddWithValue(paramName, value);
            cmd.Parameters[paramName].Direction = direction;
        }
        protected virtual object GetParameter(int index)
        {
            try
            {
                return cmd.Parameters[index].Value;
            }
            catch (Exception e)
            {
                return null;
            }

        }
        protected virtual object GetParameter(string paramName)
        {
            try
            {
                return cmd.Parameters[paramName].Value;
            }
            catch (Exception e)
            {
                return null;
            }
        }

        protected virtual void ClareParameter()
        {
            cmd.Parameters.Clear();
        }
    }

    public abstract class DB_Base_Tran : DB_Base
    {
        public int numErrorCode;
        public string frk_strErrorText;
        public int frk_isRequiresNewTransaction;

        public DB_Base_Tran()
        {
            frk_isRequiresNewTransaction = 1;
        }

        protected override void InitFrameParameter()
        {
            ClareParameter();
            AddParameter("RETURN_VALUE", SqlDbType.Int, ParameterDirection.ReturnValue, 4, DBNull.Value);
            AddParameter("@numErrorCode", SqlDbType.Int, ParameterDirection.Output, 4, DBNull.Value);

        }
        protected override void GetFrameParameter()
        {
            nReturn = Convert.ToInt32(GetParameter("Result"));
            //numErrorCode = Convert.ToInt32(GetParameter("@numErrorCode"));
        }
    }

    public abstract class DB_Base_NoTran : DB_Base
    {
        public int numErrorCode;
        public string frk_strErrorText;

        protected override void InitFrameParameter()
        {
            ClareParameter();
            //AddParameter("RETURN_VALUE", SqlDbType.Int, ParameterDirection.ReturnValue, 4, DBNull.Value);
            //AddParameter("@numErrorCode", SqlDbType.Int, ParameterDirection.Output, 4, DBNull.Value);

        }
        protected override void GetFrameParameter()
        {
            nReturn = Convert.ToInt32(GetParameter("Result"));
            //numErrorCode = Convert.ToInt32(GetParameter("@numErrorCode"));
        }
    }

    public struct DB_Result
    {
        public int nReturn;
        public int errorCode;
        public string errorMessage;
    }
}