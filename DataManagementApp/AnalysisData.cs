using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WeatherDataRequest
{
    public class AnalysisData
    {
        public string SiteDateTime { get; set; }

        public int Cloads { get; set; }

        private float temperature;

        public float Temperature
        {
            get
            {
                if (temperature > 80)
                {
                    return temperature - 273;
                }
                else
                {
                    return temperature;
                }
            }
            set {  temperature = value; }
        }

        public double GlobalRadiationHorizontal { get; set; }

        public double DiffuseRadiationHorizontal { get; set; }

        public double GlobaleRadiationTiltedPlane { get; set; }

        public double DiffuseRadiationTiltedPlane { get; set; }

        public double SolarAzimuth { get; set; }

        public double HeightOfSun { get; set; }

        public double PredictedElecGenerated { get; set; }
    }
}
