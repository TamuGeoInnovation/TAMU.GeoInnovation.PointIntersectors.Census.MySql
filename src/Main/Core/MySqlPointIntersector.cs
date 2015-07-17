using System;
using System.Data;
using System.Data.SqlClient;
using TAMU.GeoInnovation.PointIntersectors.Census.Census2010;
using USC.GISResearchLab.AddressProcessing.Core.Standardizing.StandardizedAddresses.Lines.LastLines;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Utils.Databases;
using TAMU.GeoInnovation.PointIntersectors.Census.PointIntersecters.AbstractClasses;

namespace TAMU.GeoInnovation.PointIntersectors.Census.MySql.Core
{

    [Serializable]
    public class MySqlPointIntersector : AbstractPointIntersector
    {

        #region Properties



        #endregion

        public MySqlPointIntersector()
            : base()
        { }

        public MySqlPointIntersector(IQueryManager referenceDataQueryManager)
            : base(referenceDataQueryManager)
        { }


        public override Object GetRecord(double longitude, double latitude, string table, string shapeField)
        {

            DataTable ret = null;

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {

                    string sql = "";

                    sql += " SELECT ";
                    sql += "  * ";
                    sql += " FROM ";
                    sql += "'" + table + "'";
                    sql += " WHERE ";


                    sql += "  Contains(" + shapeField + ", GeomFromText('POINT(?latitude ?longitude)')) = 1";

                    SqlCommand cmd = new SqlCommand(sql);

                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                    IQueryManager qm = ReferenceDataQueryManager;
                    qm.AddParameters(cmd.Parameters);
                    ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetRecord: " + e.Message, e);
            }

            return ret;
        }

        public override Object GetNearestRecord(double longitude, double latitude, string table, string shapeField, double maxDistance)
        {
        
            DataTable ret = null;

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {

                    string sql = "";

                    sql += " SELECT ";
                    sql += "  TOP 1 ";
                    sql += "  stateFp10, ";
                    sql += "  countyFp10, ";
                    sql += "  tractCe10, ";
                    sql += "  blockCe10, ";
                    sql += "  GeoId10, ";

                    sql += "  st_distance(shapeGeom, GeomFromText('POINT(?latitude ?longitude)')) as dist";
                    sql += " FROM ";
                    sql += "'" + table + "'";
                    sql += " WITH (INDEX (idx_geog))";
                    sql += " WHERE ";
                    sql += " st_distance(" + shapeField + ", GeomFromText('POINT(?latitude ?longitude)')) <= ?distanceThreshold ";




                    sql += "  ORDER BY ";
                    sql += "  dist ";

                    SqlCommand cmd = new SqlCommand(sql);
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude1", SqlDbType.Decimal, latitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude1", SqlDbType.Decimal, longitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude2", SqlDbType.Decimal, latitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude2", SqlDbType.Decimal, longitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("distanceThreshold", SqlDbType.Decimal, maxDistance));

                    IQueryManager qm = ReferenceDataQueryManager;
                    qm.AddParameters(cmd.Parameters);
                    ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetNearestRecord: " + e.Message, e);
            }

            return ret;
        }




    }


}

  