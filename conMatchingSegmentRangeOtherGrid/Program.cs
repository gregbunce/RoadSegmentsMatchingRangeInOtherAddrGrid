using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;

namespace conMatchingSegmentRangeOtherGrid
{
    class Program
    {
        static FileStream fileStream;
        static StreamWriter streamWriter;

        // pass in one of the following pair of command line args: STREETNAME STREETTYPE or ALIAS1 ALIAS1TYPE or ALIAS2 ALIAS2TYPE
        static void Main(string[] args)
        {
            try
            {
                // get access to the date and time for the text file name
                string strYearMonthDayHourMin = DateTime.Now.ToString("-yyyy-MM-dd-HH-mm");

                // create sql query string for recordset to loop through (remove the top(#) keyword when running outside of testing)
                string strSqlQuery = @"select top(1000) 
                                    SGID10.Transportation.ROADS.GLOBALID, 
                                    SGID10.Transportation.ROADS.ADDR_SYS, 
                                    SGID10.Transportation.ROADS.L_F_ADD, 
                                    SGID10.Transportation.ROADS.L_T_ADD, 
                                    SGID10.Transportation.ROADS.R_F_ADD, 
                                    SGID10.Transportation.ROADS.R_T_ADD, 
                                    SGID10.Transportation.ROADS.PREDIR, 
                                    SGID10.Transportation.ROADS." + args[0] + @", 
                                    SGID10.Transportation.ROADS." + args[1] + @",
                                    SGID10.Transportation.ROADS.SUFDIR
                                    from SGID10.Transportation.ROADS                                    
                                    where CARTOCODE not in ('1','7','99')                                    
                                    and (HWYNAME = '')                                    
                                    and ((L_F_ADD <> 0 and L_T_ADD <> 0) OR (R_F_ADD <> 0 and R_T_ADD <> 0))                                    
                                    and (SGID10.Transportation.ROADS." + args[0] + @" like '%[A-Z]%')                                    
                                    and (SGID10.Transportation.ROADS." + args[0] + @" <> '')                                    
                                    and (SGID10.Transportation.ROADS." + args[0] + @" not like '%ROUNDABOUT%')                                    
                                    and (SGID10.Transportation.ROADS." + args[0] + @" not like '% SB')                                    
                                    and (SGID10.Transportation.ROADS." + args[0] + @" not like '% NB')                                                          
                                    order by SGID10.Transportation.ROADS." + args[0] + @", SGID10.Transportation.ROADS." + args[1] + @", SGID10.Transportation.ROADS.ADDR_SYS;";

                //setup a file stream and a stream writer to write out the road segments
                string path = @"C:\temp\MatchingRoadSegOtherAddrGrid_" + args[0] + strYearMonthDayHourMin + ".txt";
                fileStream = new FileStream(path, FileMode.Create);
                streamWriter = new StreamWriter(fileStream);
                // write the first line of the text file - this is the field headings
                streamWriter.WriteLine("CODE_ID" + "," + "GLOBALID" + "," + "L_F_ADD" + "," + "L_T_ADD" + "," + "R_F_ADD" + "," + "R_T_ADD" + "," + "ADDR_SYS" + "," + "PREDIR" + "," + args[0] + "," + args[1] + "," + "SUFDIR" + "," + "RangeMatch_N" + "," + "RangeMatch_S" + "," + "RangeMatch_E" + "," + "RangeMatch_W" + "," + "NOTES");
                int intIttrID = 0;

                // get connection string to sql database from appconfig
                var connectionString = ConfigurationManager.AppSettings["myConn"];

                                // get a record set of road segments that need assigned predirs 
                using (SqlConnection con1 = new SqlConnection(connectionString))
                {
                    // open the sqlconnection
                    con1.Open();

                    // create a sqlcommand - allowing for a subset of records from the table
                    using (SqlCommand command1 = new SqlCommand(strSqlQuery, con1))

                    // create a sqldatareader
                    using (SqlDataReader reader1 = command1.ExecuteReader())
                    {
                        if (reader1.HasRows)
                        {
                            // loop through the record set
                            while (reader1.Read())
                            {
                                // itterate the row count
                                intIttrID = intIttrID + 1;

                                string strAddrSystem = reader1["ADDR_SYS"].ToString();
                                string strSufDir = reader1["SUFDIR"].ToString();
                                string strGlobalId = reader1["GLOBALID"].ToString();
                                int intL_F = Convert.ToInt32(reader1["L_F_ADD"]);
                                int intL_T = Convert.ToInt32(reader1["L_T_ADD"]);
                                int intR_F = Convert.ToInt32(reader1["R_F_ADD"]);
                                int intR_T = Convert.ToInt32(reader1["R_T_ADD"]);
                                string strPreDir = reader1["PREDIR"].ToString();
                                string strStreetName = reader1[args[0]].ToString();
                                string strStreetType = reader1[args[1]].ToString();

                                // check if this road segment with this range can be found in another quad, within this address system
                                Tuple<string, string, string, string, string> tplSegsInOtherGrids = RoadSegmentInOtherGrid(intL_F, intL_T, intR_F, intR_T, strAddrSystem, strStreetName, strStreetType, strPreDir, args[0], args[1], strSufDir);

                                // check if any values were returned
                                if (tplSegsInOtherGrids.Item1 != "-1" & tplSegsInOtherGrids.Item2 != "-1" & tplSegsInOtherGrids.Item3 != "-1" & tplSegsInOtherGrids.Item4 != "-1" & tplSegsInOtherGrids.Item5 != "-1")
                                {
                                    // 0=off; 1=on in the RangeMatch fields
                                    streamWriter.WriteLine(intIttrID + ",{" + strGlobalId.ToUpper() + "}," + intL_F + "," + intL_T + "," + intR_F + "," + intR_T + "," + strAddrSystem + "," + strPreDir + "," + strStreetName + "," + strStreetType + "," + strSufDir + "," + tplSegsInOtherGrids.Item1 + "," + tplSegsInOtherGrids.Item2 + "," + tplSegsInOtherGrids.Item3 + "," + tplSegsInOtherGrids.Item4 + "," + tplSegsInOtherGrids.Item5);
                                }
                                else
                                {
                                    // 0=off; 1=on in the RangeMatch fields
                                    streamWriter.WriteLine(intIttrID + ",{" + strGlobalId.ToUpper() + "}," + intL_F + "," + intL_T + "," + intR_F + "," + intR_T + "," + strAddrSystem + "," + strPreDir + "," + strStreetName + "," + strStreetType + "," + strSufDir + "," + string.Empty + "," + string.Empty + "," + string.Empty + "," + string.Empty + "," + string.Empty);
                                }
                            }
                        }
                    }
                }
                //close the stream writer
                streamWriter.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine("There was an error with the conMatchingSegmentRangeOtherGrid console application, in the Main method." + ex.Message + " " + ex.Source + " " + ex.InnerException + " " + ex.HResult + " " + ex.StackTrace + " " + ex);
                Console.ReadLine();
            }
        }



        // this method checks if the passed-in road segment is found in another address grid/quad (in the same address system) with overlapping ranges
        static Tuple<string, string, string, string, string> RoadSegmentInOtherGrid(int intL_F, int intL_T, int intR_F, int intR_T, string strAddrSys, string strStName, string strStType, string strPreDir, string strArg0, string strArg1, string strSufDir)
        {
            try
            {
                string strPreDir_xN = string.Empty;
                string strPreDir_xS = string.Empty;
                string strPreDir_xE = string.Empty;
                string strPreDir_xW = string.Empty;
                string strPreDir_xOther = string.Empty;

                string strQueryStringMatchOtherGrid = @"select SGID10.Transportation.ROADS.PREDIR, SGID10.Transportation.ROADS.GLOBALID 
                                                    from SGID10.Transportation.ROADS where 
                                                    SGID10.Transportation.ROADS.ADDR_SYS = '" + strAddrSys + @"'
                                                    and SGID10.Transportation.ROADS." + strArg0 + @" = '" + strStName + @"'
                                                    and SGID10.Transportation.ROADS." + strArg1 + @" = '" + strStType + @"'
                                                    and SGID10.Transportation.ROADS.PREDIR <> '" + strPreDir + @"'
                                                    and SGID10.Transportation.ROADS.SUFDIR = '" + strSufDir + @"'
                                                    and ((SGID10.Transportation.ROADS.L_F_ADD >= " + intL_F + @" and SGID10.Transportation.ROADS.L_T_ADD <= " + intL_T + @") or (SGID10.Transportation.ROADS.R_F_ADD >= " + intR_F + @" and SGID10.Transportation.ROADS.R_T_ADD <= " + intR_T + @"))";

                // get connection string to sql database from appconfig
                var connectionString = ConfigurationManager.AppSettings["myConn"];

                // get a record set of road segments that need assigned predirs 
                using (SqlConnection con2 = new SqlConnection(connectionString))
                {
                    // open the sqlconnection
                    con2.Open();

                    // create a sqlcommand - allowing for a subset of records from the table
                    using (SqlCommand command2 = new SqlCommand(strQueryStringMatchOtherGrid, con2))

                    // create a sqldatareader
                    using (SqlDataReader reader2 = command2.ExecuteReader())
                    {
                        if (reader2.HasRows)
                        {
                            // loop through the record set
                            while (reader2.Read())
                            {
                                switch (reader2["PREDIR"].ToString().ToUpper().Trim())
                                {
                                    case "N":
                                        strPreDir_xN = "1";
                                        break;
                                    case "S":
                                        strPreDir_xS = "1";
                                        break;
                                    case "E":
                                        strPreDir_xE = "1";
                                        break;
                                    case "W":
                                        strPreDir_xW = "1";
                                        break;
                                    default:
                                        strPreDir_xOther = reader2["PREDIR"].ToString();
                                        break;
                                }
                            }
                        }
                        else
                        {
                            strPreDir_xN = "-1";
                            strPreDir_xS = "-1";
                            strPreDir_xE = "-1";
                            strPreDir_xW = "-1";
                            strPreDir_xOther = "-1";
                        }
                    }
                }
                return Tuple.Create(strPreDir_xN, strPreDir_xS, strPreDir_xE, strPreDir_xW, strPreDir_xOther);
            }
            catch (Exception ex)
            {
                Console.WriteLine("There was an error with the conMatchingSegmentRangeOtherGrid console application, in the RoadSegmentInOtherGrid method." + ex.Message + " " + ex.Source + " " + ex.InnerException + " " + ex.HResult + " " + ex.StackTrace + " " + ex);
                Console.ReadLine();
                return Tuple.Create("-1", "-1", "-1", "-1", "-1");
            }
        }



    }
}
