import requests
import pandas as pd
import json
from cryptography.fernet import Fernet
from spacy.matcher import PhraseMatcher
import spacy
from unidecode import unidecode
import argparse


class ApiCaller:

    def __init__(self, url, alerts, key, secret):
        self.data = self.makedataframe(self.makeapicall(url, self.getapikey(key, secret)), alerts)

    @staticmethod
    def getapikey(encryptionkeypath: str, secretkeypath: str):
        enckey = open(encryptionkeypath, "rb").read()
        f = Fernet(enckey)
        with open(secretkeypath, "rb") as file:
            # read the encrypted data
            encrypted_data = file.read()
        # decrypt data
        return f.decrypt(encrypted_data).decode()

    @staticmethod
    def makeapicall(url: str, decryptedkey: str):
        payload = {'key': decryptedkey}

        try:
            r = requests.get(url, params=payload)
            alerts = json.dumps(r.json(), indent=4)
            print('Data Downloaded from API!')
            return alerts

        except requests.exceptions.HTTPError as errh:
            print(errh)
        except requests.exceptions.ConnectionError as errc:
            print(errc)
        except requests.exceptions.Timeout as errt:
            print(errt)
        except requests.exceptions.RequestException as err:
            print(err)

    @staticmethod
    def makedataframe(jsondata: str, alerts = False):
        dataframe = pd.read_json(jsondata)
        if alerts is True:
            alertsflattened = []
            for i, row in dataframe.iterrows():
                flattenedcontentdf = pd.json_normalize(dataframe['contents'][i])
                flattenedcontentdf['id'] = row['id']
                rowdf = pd.DataFrame(row).transpose()
                mergeddf = pd.merge(rowdf,flattenedcontentdf, how="outer", on="id")
                mergeddf = mergeddf.drop(columns='contents')
                alertsflattened.append(mergeddf)
            dataframe = pd.concat(alertsflattened).reset_index()
            dataframe = dataframe[dataframe['text'].notna()]
            dataframe['language'] = dataframe['language'].apply(lambda x: unidecode(x))
        return dataframe


class LanguageProcessor:

    def __init__(self, languagecode, spacymodel, querydataframe):
        self.languagemodel = spacy.load(spacymodel)
        self.data = querydataframe
        self.languagecode = languagecode
        self.zippeddata = self.zipqueryterms()
        self.phrasematcher = self.createphrasematcher()

    def zipqueryterms(self):
        return list(zip(list(self.data[(self.data['language'] == self.languagecode)].id),
                    list(self.data[(self.data['language'] == self.languagecode)].text),
                    list(self.data[(self.data['language'] == self.languagecode)].keepOrder)))

    def createphrasematcher(self):
        matcherobject = PhraseMatcher(self.languagemodel.vocab, attr="LOWER")
        for id, term, order in self.zippeddata:
            matcherobject.add(str(id), [self.languagemodel.make_doc(term.lower())])
            if order is False:
                matcherobject.add(str(id), [self.languagemodel.make_doc(" ".join(term.lower().split(" ")[::-1]))])
        return matcherobject


class AlertsProcessor:

    def __init__(self, alertsdataframe, languageprocessoreng, languageprocessorspa, languageprocessordeu, languageprocessorita, filename):
        self.data = alertsdataframe
        self.es = languageprocessorspa
        self.de = languageprocessordeu
        self.it = languageprocessorita
        self.en = languageprocessoreng
        self.matchesdataset = []
        self.filename = filename


    def datasetiterator(self):
        for i, row in self.data.iterrows():
            if row.language == 'es':
                self.tokenizematchandappend(row, self.es.phrasematcher, self.es.languagemodel)
            elif row.language == 'de':
                self.tokenizematchandappend(row, self.de.phrasematcher, self.de.languagemodel)
            elif row.language == 'it':
                self.tokenizematchandappend(row, self.it.phrasematcher, self.it.languagemodel)
            else:
                self.tokenizematchandappend(row, self.en.phrasematcher, self.en.languagemodel)


    @staticmethod
    def tokenizetext(text: str, languagemodel: spacy.Language):
        return [t.text.lower() for t in languagemodel(text) if (not t.is_stop and not t.is_punct)]


    @staticmethod
    def matchandreturn(tokenizedtext: list, phrasematcher: spacy.matcher.PhraseMatcher, tokenizedtextid: str, languagemodel: spacy.Language):
        matcheslist = []
        matches = phrasematcher(languagemodel(" ".join(tokenizedtext)), as_spans=True)
        for match in matches:
            matcheslist.append((tokenizedtextid,match.label_,match.text))
        return set(matcheslist)


    def tokenizematchandappend(self, row: pd.Series, phrasematcher: spacy.matcher.PhraseMatcher, languagemodel: spacy.Language):
        tokensentence = self.tokenizetext(row.text, languagemodel)
        matchesset = self.matchandreturn(tokensentence, phrasematcher, row.id, languagemodel)
        for matchset in matchesset:
            self.matchesdataset.append(matchset)

    def listtodataframe(self):
        self.matchesdataset = pd.DataFrame(self.matchesdataset, columns=['alertid', 'queryid', 'text'])

    def dumpmatchestofile(self):
        self.listtodataframe()
        with open(self.filename, "w+", encoding='utf-8') as f:
            f.write(self.matchesdataset.to_json(orient='index', force_ascii=False))


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--filename", help="name of the file in output", required=True)
    args = parser.parse_args()
    queries = ApiCaller('https://services.prewave.ai/adminInterface/api/testQueryTerm', False, "key.key", "secretkey.key")
    alerts = ApiCaller('https://services.prewave.ai/adminInterface/api/testAlerts', True, "key.key", "secretkey.key")
    engproc = LanguageProcessor('en', 'en_core_web_sm', queries.data)
    spaproc = LanguageProcessor('es', 'es_core_news_sm', queries.data)
    deuproc = LanguageProcessor('de', 'de_core_news_sm', queries.data)
    itaproc = LanguageProcessor('it', 'it_core_news_sm', queries.data)
    process = AlertsProcessor(alerts.data, engproc, spaproc, deuproc, itaproc, args.filename)
    process.datasetiterator()
    process.dumpmatchestofile()
    print("JSON successfully created!")