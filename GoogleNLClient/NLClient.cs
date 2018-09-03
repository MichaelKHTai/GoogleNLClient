using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Language.V1;
using Grpc.Auth;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Grpc.Core;
using Google.Api.Gax.Grpc;

namespace GoogleNLClient
{
    public struct NLData
    {
        public object self;
        public string[] words;
    }

    public struct NLRaw
    {
        public object self;
        public string word;
    }
    public class NLClient
    {
        LanguageServiceClient client { get; set; }
        int limit = 0;
        public NLClient(string jsonPath)
        {
            var credential = GoogleCredential.FromFile(jsonPath).CreateScoped(LanguageServiceClient.DefaultScopes);
            var channel = new Grpc.Core.Channel(
                LanguageServiceClient.DefaultEndpoint.ToString(),
                credential.ToChannelCredentials());
            client = LanguageServiceClient.Create(channel);
        }

        public async Task<NLData[]> executeAsyncRequest(NLRaw[] data)
        {
            
            List<NLData> result = new List<NLData>();
            List<Task<NLData>> tasks = new List<Task<NLData>>();
            int count = 0;
            int data_len = data.Length;
            int left_count = data_len;
            int max_count = 500;
            while (count < data_len)
            {
                int max = Math.Min(count + max_count - limit, data_len);
                for (var i = count; i < max; i++)
                {
                    tasks.Add(runNLAnalysis(data[i]));
                }

                
                Task<NLData[]> FinishedTask = Task.WhenAll(tasks);
                NLData[] temp_data = await FinishedTask;
                result.AddRange(temp_data);
                tasks.Clear();
                
                count = count + max_count - limit;
                if (count < data_len)
                {
                    Console.WriteLine("Google NL limit met, wait a minute.");
                    await Task.Delay(60000);
                    limit = 0;
                }
                else
                {
                    limit = left_count;
                }
                left_count -= (max_count - limit);
            }
                
            return result.ToArray();
        }

        public async Task<NLData> runNLAnalysis(NLRaw element)
        {
            string content = element.word;
            if (content.Length > 1000)
            {
                content = content.Substring(0,1000);
            }
            var response = await client.AnalyzeSyntaxAsync(new Document()
            {
                Content = content,
                Type = Document.Types.Type.PlainText
            });
            List<string> current_data = new List<string>();
            foreach (var syntax in response.Tokens)
            {
                Regex pattern = new Regex(@"\W+$");
                string word = pattern.Replace(syntax.Lemma, "").ToLower();
                if (word != "")
                {
                    current_data.Add(word);
                }
            }
            NLData r = new NLData();
            r.words = current_data.ToArray();
            r.self = element.self;
            
            return r;
        }

    }

}
