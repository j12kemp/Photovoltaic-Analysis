using Microsoft.VisualBasic.FileIO;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using WeatherDataRequest;

internal class Program
{
    static string workingDirectory = @"C:\Users\jaco.kemp\Documents\DataAnalytics\PV_Project_Submission\DataFiles\";
    static string dataFile = workingDirectory + "forcast_latest.csv";
    static string metronormDataFile = workingDirectory + "site_meteonorm_data.csv";

    static List<AnalysisData> data = new List<AnalysisData>();
    static List<AnalysisData> newData = new List<AnalysisData>();


    private static void Main()
    {
        Task t = new Task(DownloadPageAsync);
        t.Start();
        Console.WriteLine("Downloading page...");
        Console.ReadLine();
        CallAnalyticalService().Wait();
        WriteUpdatedData(data);
        Console.WriteLine("New data written to file");
        Console.ReadLine();
    }

    private static async void DownloadPageAsync()
    {
        var metronormData = GetMetronormData();
        // ... Target page.
        string page = "http://api.openweathermap.org/data/2.5/forecast?id=1011632&APPID=864fcf734aa20d4ab07988407e08505b";
        var today = DateTime.Today.ToString("MM-dd-yy");
        // ... Use HttpClient.
        using (HttpClient client = new HttpClient())
        using (HttpResponseMessage response = await client.GetAsync(page))
        using (HttpContent content = response.Content)
        {
            // ... Read the string.
            string result = await content.ReadAsStringAsync();

            // ... Display the result.
            if (result != null)
            {
                UpdateWeatherDataFile(result, metronormData);
                WriteJsonWeatherDataToFile(today, result);
                Console.WriteLine("Download complete, press any key to continue");
            }
        }
    }

    private static void UpdateWeatherDataFile(string result, List<AnalysisData> metronormData)
    {
        Rootobject newWeatherData;

        List<AnalysisData> existingData = ReadPreviousData(dataFile);

        newWeatherData = JsonConvert.DeserializeObject<Rootobject>(value: result);
        List<AnalysisData> newData = ConvertToAnalyisWeatherData(newWeatherData);
        var newItems = newData.Where(x => !existingData.Any(y => x.SiteDateTime == y.SiteDateTime));
        newData = newItems.Where(x => x.GlobalRadiationHorizontal > 20).ToList();
        existingData.AddRange(newData);        
        data = existingData.Where(x => x.GlobalRadiationHorizontal > 20).ToList();
        WriteUpdatedData(data);
    }

    private static List<AnalysisData> ReadPreviousData(string dataFile)
    {
        List<AnalysisData> existingData = new List<AnalysisData>();
        using (TextFieldParser parser = new TextFieldParser(dataFile))
        {
            parser.TextFieldType = FieldType.Delimited;
            parser.SetDelimiters(",");
            bool header = true;
            while (!parser.EndOfData)
            {
                //skips header on first line
                if (header)
                {
                    string[] fields = parser.ReadFields();
                    header = false;
                }
                else
                {
                    //Process row
                    string[] fields = parser.ReadFields();
                    existingData.Add(new AnalysisData
                    {
                        SiteDateTime = fields[0] + " " + fields[1],
                        Temperature = float.Parse(fields[2]),
                        Cloads = Int32.Parse(fields[3]),
                        GlobalRadiationHorizontal = Double.Parse(fields[4]),
                        DiffuseRadiationHorizontal = Double.Parse(fields[5]),
                        GlobaleRadiationTiltedPlane = Double.Parse(fields[6]),
                        DiffuseRadiationTiltedPlane = Double.Parse(fields[7]),
                        SolarAzimuth = Double.Parse(fields[8]),
                        HeightOfSun = Double.Parse(fields[9])
                    });
                }
            }
        }
        return existingData;
    }

    private static void WriteUpdatedData(List<AnalysisData> updatedData)
    {
        StringBuilder sb = new StringBuilder("DateTime,Temperature,Cload_Cover,Global_radiation_horizontal,Diffuse_radiation_horizontal,Global_radiation_tilted_plane,Diffuse_radiation_tilted_plane,Solar_azimuth,Height_of_sun,Predicted Electricity Generated");
        sb.AppendLine();
        foreach (var i in updatedData)
        {
            sb.AppendLine(i.SiteDateTime + ',' + i.Temperature + ',' + i.Cloads + ',' + i.GlobalRadiationHorizontal + ',' +
                            i.DiffuseRadiationHorizontal + ',' + i.GlobaleRadiationTiltedPlane + ',' + i.DiffuseRadiationTiltedPlane + ',' +
                            i.SolarAzimuth + ',' + i.HeightOfSun + ',' + i.PredictedElecGenerated);
        }

        File.WriteAllText(dataFile, sb.ToString());
    }

    private static List<AnalysisData> ConvertToAnalyisWeatherData(Rootobject weatherData)
    {
        var metronormDataList = GetMetronormData();
        //month/day
        //01/01 1:00
        List<AnalysisData> weatherDataList = new List<AnalysisData>();
        foreach (var item in weatherData.list)
        {
            //2018-08-12 03:00:00
            string dateTime = item.dt_txt.Replace('-', '/').Substring(0, item.dt_txt.Length - 3);
            string[] dateTimeComp = dateTime.Split(' ');
            string[] time = dateTimeComp[1].Split(':');
            int hour = int.Parse(time[0]);
            int previousHour = hour - 1;
            int nextHour = hour + 1;
            string previousHourString = previousHour.ToString();
            string nextHourString = nextHour.ToString();
            if (previousHour < 10)
            {
                previousHourString = "0" + previousHourString;
            }
            if (nextHour < 10)
            {
                nextHourString = "0" + nextHourString;
            }
            string previousHourDateTime = dateTimeComp[0] + " " + previousHourString + ":" + time[1];
            string nextHourDateTime = dateTimeComp[0] + " " + nextHourString.ToString() + ":" + time[1];
            float temperature = item.main.temp;
            int cloads = item.clouds.all;

            AnalysisData metronormDataThisHour = metronormDataList.Where(j => j.SiteDateTime == dateTime.Remove(0, 5)).First();

            if (hour == 0)
            {
                AnalysisData metronormDataNextHour = metronormDataList.Where(j => j.SiteDateTime == nextHourDateTime.Remove(0, 5)).First();
                weatherDataList.Add(GenerateAnalysisData(dateTime, temperature, cloads, metronormDataThisHour));
                weatherDataList.Add(GenerateAnalysisData(nextHourDateTime, temperature, cloads, metronormDataNextHour));
            }
            else if (hour == 23)
            {
                AnalysisData metronormDataPreviousHour = metronormDataList.Where(j => j.SiteDateTime == previousHourDateTime.Remove(0, 5)).First();
                weatherDataList.Add(GenerateAnalysisData(previousHourDateTime, temperature, cloads, metronormDataPreviousHour));
                weatherDataList.Add(GenerateAnalysisData(dateTime, temperature, cloads, metronormDataThisHour));
            }
            else
            {
                AnalysisData metronormDataNextHour = metronormDataList.Where(j => j.SiteDateTime == nextHourDateTime.Remove(0, 5)).First();
                AnalysisData metronormDataPreviousHour = metronormDataList.Where(j => j.SiteDateTime == previousHourDateTime.Remove(0, 5)).First();
                weatherDataList.Add(GenerateAnalysisData(previousHourDateTime, temperature, cloads, metronormDataPreviousHour));
                weatherDataList.Add(GenerateAnalysisData(dateTime, temperature, cloads, metronormDataThisHour));
                weatherDataList.Add(GenerateAnalysisData(nextHourDateTime, temperature, cloads, metronormDataNextHour));
            }
        }
        return weatherDataList;
    }

    private static List<AnalysisData> GetMetronormData()
    {
        List<AnalysisData> dataList = new List<AnalysisData>();
        using (TextFieldParser parser = new TextFieldParser(metronormDataFile))
        {
            parser.TextFieldType = FieldType.Delimited;
            parser.SetDelimiters(",");
            bool header = true;
            while (!parser.EndOfData)
            {
                //skips header on first line
                if (header)
                {
                    string[] fields = parser.ReadFields();
                    header = false;
                }
                else
                {
                    //month/day/year
                    //1/1/2016 1:00
                    //Process row
                    string[] fields = parser.ReadFields();
                    string dateTime = fields[1];
                    string [] dateTimeFields = dateTime.Split(' ');
                    string date = dateTimeFields[0];
                    string time = dateTimeFields[1];
                    string[] timeFields = time.Split(':');

                    string[] dateFields = date.Split('/');

                    int day = Int32.Parse(dateFields[1]);
                    string dayString = String.Empty;
                    if (day < 10)
                        dayString = "0" + day.ToString();
                    else
                        dayString = day.ToString();

                    int month = Int32.Parse(dateFields[0]);
                    string monthString = String.Empty;
                    if (month < 10)
                        monthString = "0" + month.ToString();
                    else
                        monthString = month.ToString();

                    date = monthString + "/" + dayString;

                    int hour = Int32.Parse(timeFields[0]);
                    string hourString = String.Empty;
                    if (hour < 10)
                        hourString = "0" + hour.ToString();
                    else
                        hourString = hour.ToString();
                    time = hourString + ":00";
                   
                    dataList.Add(new AnalysisData
                    {
                        SiteDateTime = date + " " + time,
                        GlobalRadiationHorizontal = Double.Parse(fields[2]),
                        DiffuseRadiationHorizontal = Double.Parse(fields[3]),
                        GlobaleRadiationTiltedPlane = Double.Parse(fields[4]),
                        DiffuseRadiationTiltedPlane = Double.Parse(fields[5]),
                        SolarAzimuth = Double.Parse(fields[6]),
                        HeightOfSun = Double.Parse(fields[7])
                        //Global_radiation_horizontal,Diffuse_radiation_horizontal,Global_radiation_tilted_plane,Diffuse_radiation_tilted_plane,Solar_azimuth,Height_of_sun
                    });
                }
            }
        }

        return dataList;
    }

    private static AnalysisData GenerateAnalysisData(string dateTime, float temperature, int cloadCover, AnalysisData metronormData)
    {
        return new AnalysisData
        {
            SiteDateTime = dateTime,
            Temperature = temperature,
            Cloads = cloadCover,
            GlobalRadiationHorizontal = metronormData.GlobalRadiationHorizontal,
            DiffuseRadiationHorizontal = metronormData.DiffuseRadiationHorizontal,
            GlobaleRadiationTiltedPlane = metronormData.GlobaleRadiationTiltedPlane,
            DiffuseRadiationTiltedPlane = metronormData.DiffuseRadiationTiltedPlane,
            SolarAzimuth = metronormData.SolarAzimuth,
            HeightOfSun = metronormData.HeightOfSun
        };
    }

    private static async Task CallAnalyticalService()
    {
        using (var client = new HttpClient())
        {
            Console.WriteLine("Analys Start : wait...");
            const string apiKey = "Flza4LQye5lNboivF0jN/KT35jphDkgvRS8EJyQUF/PQZvyze5+Jb+kcSYjqCjiLLVkrz/ktglOtIj7+wKHtug=="; // Replace this with the API key for the web service
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
            client.BaseAddress = new Uri("https://ussouthcentral.services.azureml.net/workspaces/90aacf5c342c46488995379f1a16a03d/services/8de4be7f87ba47cda635072a39dd9564/execute?api-version=2.0&format=swagger");

            foreach (var item in data)
            {
                Thread.Sleep(1000);
                var scoreRequest = new
                {
                    Inputs = new Dictionary<string, List<Dictionary<string, string>>>() {
                        {
                            "input1",
                            new List<Dictionary<string, string>>(){new Dictionary<string, string>(){
                                            {
                                                "Column 0", "0"
                                            },
                                            {
                                                "Electricity_Generated", "0"
                                            },
                                            {
                                                "Temperature", item.Temperature.ToString()
                                            },
                                            {
                                                "Cload_Cover", item.Cloads.ToString()
                                            },
                                            {
                                                "Global_radiation_horizontal", item.GlobalRadiationHorizontal.ToString()
                                            },
                                            {
                                                "Diffuse_radiation_horizontal", item.DiffuseRadiationHorizontal.ToString()
                                            },
                                            {
                                                "Global_radiation_tilted_plane", item.GlobaleRadiationTiltedPlane.ToString()
                                            },
                                            {
                                                "Diffuse_radiation_tilted_plane", item.DiffuseRadiationTiltedPlane.ToString()
                                            },
                                            {
                                                "Solar_azimuth", item.SolarAzimuth.ToString()
                                            },
                                            {
                                                "Height_of_sun", item.HeightOfSun.ToString()
                                            },
                                }
                            }
                        },
                    },
                    GlobalParameters = new Dictionary<string, string>()
                    {
                    }
                };


                // WARNING: The 'await' statement below can result in a deadlock
                // if you are calling this code from the UI thread of an ASP.Net application.
                // One way to address this would be to call ConfigureAwait(false)
                // so that the execution does not attempt to resume on the original context.
                // For instance, replace code such as:
                //      result = await DoSomeTask()
                // with the following:
                //      result = await DoSomeTask().ConfigureAwait(false)

                HttpResponseMessage response = await client.PostAsJsonAsync("", scoreRequest);

                if (response.IsSuccessStatusCode)
                {
                    string result = await response.Content.ReadAsStringAsync();
                    item.PredictedElecGenerated = ConvertResult(result);
                }
                else
                {
                    Console.WriteLine(string.Format("The request failed with status code: {0}", response.StatusCode));

                    // Print the headers - they include the requert ID and the timestamp,
                    // which are useful for debugging the failure
                    Console.WriteLine(response.Headers.ToString());

                    string responseContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine(responseContent);
                }

            }
        }
    }

    private static double ConvertResult(string result)
    {
        string regexPattern = "Scored Labels.*?}";
        Match match = Regex.Match(result, regexPattern);
        string[] text = match.Value.Split('"');
        return Double.Parse(text[2]);
    }

    private static void WriteJsonWeatherDataToFile(string today, string result)
    {
        string fileStorage = workingDirectory + $@"forcast_{today}.json";
        File.WriteAllText(fileStorage, result);
    }
}
