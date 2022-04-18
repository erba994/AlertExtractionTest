import pandas as pd
from main import ApiCaller, LanguageProcessor, AlertsProcessor

if __name__ == "__main__":
    queries = ApiCaller('https://services.prewave.ai/adminInterface/api/testQueryTerm', False, "key.key", "secretkey.key")
    alerts = ApiCaller('https://services.prewave.ai/adminInterface/api/testAlerts', True, "key.key", "secretkey.key")
    engproc = LanguageProcessor('en', 'en_core_web_sm', queries.data)
    spaproc = LanguageProcessor('es', 'es_core_news_sm', queries.data)
    deuproc = LanguageProcessor('de', 'de_core_news_sm', queries.data)
    itaproc = LanguageProcessor('it', 'it_core_news_sm', queries.data)
    process = AlertsProcessor(alerts.data, engproc, spaproc, deuproc, itaproc, "")
    process.datasetiterator()
    process.listtodataframe()
    pd.options.display.max_colwidth = None
    pd.options.display.max_columns = None
    pd.options.display.width = 2000
    with open("testreport.txt", "w+", encoding="utf-8") as f:
        print("QUERY TERMS")
        print(queries.data[["text", "id", "language"]])
        print("------------------")
        print("ALERTS")
        print(alerts.data[["text", "id", "language"]])
        print("------------------")
        print("MATCHES")
        print("------------------")
        for i, row in process.matchesdataset.iterrows():
            print("ALERT ID")
            print(row.alertid)
            print("ALERT TEXT")
            print(alerts.data[(alerts.data['id'] == row.alertid)].text)
            print("QUERY TERM ID")
            print(row.queryid)
            print("QUERY TERM MATCHED")
            print(row.text)
            print("------------------")