using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Oracle.ManagedDataAccess.Client;
using DBHelper.BaseHelper;
using CommonLib;
using DBHelper.Common;

namespace OracleHelper
{
    public class OracleHelper : DBHelper.DBHelper
    {
        /// <summary>
        /// Oracle数据库操作类
        /// </summary>
        /// <param name="connectionString">oracle数据库连接字符串</param>
        public OracleHelper(string connectionString) : base(connectionString)
        {
            Environment.SetEnvironmentVariable("NLS_LANG", "SIMPLIFIED CHINESE_CHINA.ZHS16GBK", EnvironmentVariableTarget.Process);
        }

        protected override void PrepareCommandEx(DbCommand cmd, DbConnection conn, string cmdText, DbTransaction trans, CommandType cmdType, params DbParameter[] cmdParameters)
        {
            OracleCommand ocmd = cmd as OracleCommand;
            if (ocmd != null)
            {
                ocmd.BindByName = true;  //根据变量名绑定
            }
        }

        public override DataSet Query(string sqlString, params DbParameter[] dbParameter)
        {
            DataSet ds = new DataSet("ds");
            using (OracleConnection connection = new OracleConnection(_connectionString))
            {
                try
                {
                    using (OracleCommand cmd = new OracleCommand())
                    {
                        PrepareCommand(cmd, connection, sqlString, null, CommandType.Text, dbParameter);
                        using (OracleDataAdapter command = new OracleDataAdapter())
                        {
                            command.SelectCommand = cmd;
                            command.Fill(ds, "dt");
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (System.Diagnostics.Debugger.IsAttached)
                        throw new Exception(ex.Message);
                    else
                        LogHelper.Error(ex, "OracleHelper.Query");
                }
            }
            return ds;
        }

        public override object QueryScalar(string sqlString, CommandType cmdType = CommandType.Text, params DbParameter[] dbParameter)
        {
            using (OracleConnection connection = new OracleConnection(_connectionString))
            {
                using (OracleCommand cmd = new OracleCommand())
                {
                    PrepareCommand(cmd, connection, sqlString, null, cmdType, dbParameter);
                    object obj = cmd.ExecuteScalar();
                    if ((Equals(obj, null)) || (Equals(obj, DBNull.Value)))
                    {
                        return null;
                    }
                    else
                    {
                        return obj;
                    }
                }
            }
        }

        public override string GetPageRowNumSql(string dataSql, int startRowNum, int endRowNum)
        {
            return string.Format("select * from (select ROWNUM rowno,z_.* from ({0}) z_ where ROWNUM <= {2}) where rowno > {1}", dataSql, startRowNum, endRowNum);
        }

        public override string GetRowLimitSql(string dataSql, int rowLimit)
        {
            return string.Format("select * from ({0}) where rownum<={1}", dataSql, rowLimit);
        }

        public override DbDataReader ExecuteReader(string sqlString, params DbParameter[] dbParameter)
        {
            OracleConnection conn = new OracleConnection();
            conn.ConnectionString = ConnectionString;
            OracleCommand cmd = new OracleCommand();
            OracleDataReader rdr = null;
            try
            {
                //Prepare the command to execute
                PrepareCommand(cmd, conn, sqlString, null, CommandType.Text, dbParameter);
                rdr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                return rdr;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex);
                rdr?.Close();
                cmd.Dispose();
                conn.Close();
            }
            return null;
        }

        public override int ExecuteNonQuery(CommandInfo cmdInfo)
        {
            //Create a connection
            using (OracleConnection connection = new OracleConnection())
            {
                try
                {
                    connection.ConnectionString = ConnectionString;
                    // Create a new Oracle command
                    OracleCommand cmd = new OracleCommand();
                    //Prepare the command
                    PrepareCommand(cmd, connection, cmdInfo.Text, null, cmdInfo.Type, cmdInfo.Parameters);

                    //Execute the command
                    int val = cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    return val;
                }
                catch (Exception ex)
                {
                    if (Debugger.IsAttached)
                        throw new Exception(ex.Message);
                    else
                        LogHelper.Error(ex, "OracleHelper.ExecuteNonQuery");
                    return -1;
                }
            }
        }

        public override int ExecuteProcedure(string storedProcName, params DbParameter[] parameters)
        {
            using (OracleConnection conn = new OracleConnection())
            {
                conn.ConnectionString = ConnectionString;
                OracleCommand cmd = new OracleCommand();
                try
                {
                    PrepareCommand(cmd, conn, storedProcName, null, CommandType.StoredProcedure, parameters);
                    int i = cmd.ExecuteNonQuery();
                    return i;
                }
                catch (Exception ex)
                {
                    if (Debugger.IsAttached)
                        throw new Exception(ex.Message);
                    else
                        LogHelper.Error(ex, "OracleHelper.ExecuteProcedure");
                    return -1;
                }
                finally
                {
                    cmd.Dispose();
                }
            }
        }

        public override int ExecuteProcedureTran(string storedProcName, params DbParameter[] parameters)
        {
            using (OracleConnection conn = new OracleConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                OracleTransaction tran = conn.BeginTransaction();
                OracleCommand cmd = new OracleCommand();
                try
                {
                    PrepareCommand(cmd, conn, storedProcName, tran, CommandType.StoredProcedure, parameters);
                    int i = cmd.ExecuteNonQuery();
                    tran.Commit();
                    return i;
                }
                catch (Exception ex)
                {
                    tran.Rollback();
                    if (Debugger.IsAttached)
                        throw new Exception(ex.Message);
                    else
                        LogHelper.Error(ex, "OracleHelper.ExecuteProcedureTran");
                    return -1;
                }
                finally
                {
                    tran.Dispose();
                    cmd.Dispose();
                }
            }
        }

        public override int ExecuteSqlTran(CommandInfo cmdInfo)
        {
            //Create a connection
            using (OracleConnection connection = new OracleConnection())
            {
                connection.ConnectionString = ConnectionString;
                connection.Open();
                OracleTransaction tran = connection.BeginTransaction();
                try
                {
                    // Create a new Oracle command
                    OracleCommand cmd = new OracleCommand();
                    //Prepare the command
                    PrepareCommand(cmd, connection, cmdInfo.Text, tran, cmdInfo.Type, cmdInfo.Parameters);

                    //Execute the command
                    int val = cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    return val;
                }
                catch (Exception ex)
                {
                    tran.Rollback();
                    if (Debugger.IsAttached)
                        throw new Exception(ex.Message);
                    else
                        LogHelper.Error(ex, "OracleHelper.ExecuteSqlTran");
                    return -1;
                }
                finally
                {
                    tran.Dispose();
                }
            }
        }

        public override int ExecuteSqlsTran(List<CommandInfo> cmdList, int num = 5000)
        {
            if (cmdList == null || cmdList.Count == 0) return -1;

            using (OracleConnection conn = new OracleConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                int allCount = 0;

                //Stopwatch watch = new Stopwatch();
                while (cmdList.Count > 0)
                {
                    //watch.Reset();
                    //watch.Start();                
                    var submitSQLs = cmdList.Take(num);
                    OracleTransaction tx = conn.BeginTransaction();
                    OracleCommand cmd = new OracleCommand();
                    int count = 0;
                    try
                    {
                        foreach (CommandInfo c in submitSQLs)
                        {
                            try
                            {
                                if (!string.IsNullOrEmpty(c.Text))
                                {
                                    PrepareCommand(cmd, conn, c.Text, tx, c.Type, c.Parameters);
                                    int res = cmd.ExecuteNonQuery();
                                    if (c.EffentNextType == EffentNextType.ExcuteEffectRows && res == 0)
                                    {
                                        throw new Exception("Oracle:违背要求" + c.Text + "必须有影响行");
                                    }
                                    count += res;
                                }
                            }
                            catch (Exception ex)
                            {
                                if (c.FailRollback)
                                    throw ex;
                            }
                        }
                        tx.Commit();
                    }
                    catch (Exception ex)
                    {
                        tx.Rollback();
                        if (Debugger.IsAttached)
                            throw new Exception(ex.Message);
                        else
                            LogHelper.Error(ex, "OracleHelper.ExecuteSqlsTran");
                        count = 0;
                        break;
                    }
                    finally
                    {
                        cmd.Dispose();
                        tx.Dispose();
                        allCount += count;
                    }

                    int removeCount = cmdList.Count >= num ? num : cmdList.Count; //每次最多执行1000行
                    cmdList.RemoveRange(0, removeCount);
                    //watch.Stop();
                    //Console.WriteLine(cmdList.Count + "-" + allCount + "-" + watch.ElapsedMilliseconds / 1000);
                }
                return allCount;
            }
        }

        /// <summary>
        /// 测试连接字符串是否正常
        /// </summary>
        /// <param name="connstr"></param>
        /// <returns></returns>
        public override bool TestConnectionString()
        {
            DataTable dt = QueryTable("select 1 from dual");
            if (dt.IsNotEmpty())
            {
                if (dt.Rows[0][0].ToString() == "1")
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// 获取序列的当前值
        /// </summary>
        /// <param name="sqName"></param>
        /// <returns></returns>
        public int GetCurrValFormSQName(string sqName)
        {
            int val = 0;
            if (string.IsNullOrWhiteSpace(sqName))
            {
                return val;
            }

            object obj = QueryScalar(string.Format("select {0}.currval from dual", sqName));
            if (obj != null && !string.IsNullOrWhiteSpace(obj.ToString()))
            {
                val = Convert.ToInt32(obj);
            }
            return val;
        }
        /// <summary>
        /// 获取序列的下一个值
        /// </summary>
        /// <param name="sqName"></param>
        /// <returns></returns>
        public int GetNextValFormSQName(string sqName)
        {
            int val = 0;
            if (string.IsNullOrWhiteSpace(sqName))
            {
                return val;
            }

            object obj = QueryScalar(string.Format("select {0}.nextval from dual", sqName));
            if (obj != null && !string.IsNullOrWhiteSpace(obj.ToString()))
            {
                val = Convert.ToInt32(obj);
            }
            return val;
        }

        public override DataBaseType GetCurrentDataBaseType()
        {
            return DataBaseType.Oracle;
        }

        public override bool TableExists(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) return false;

            object count = QueryScalar($"SELECT count(*) FROM All_Tables WHERE table_name = '{tableName}'");
            int nCount = Convert.ToInt32(count);
            return nCount > 0;
        }
    }
}