import gensim.downloader as api
from gensim.models import KeyedVectors
from transformers import AutoTokenizer, AutoModel
import torch
from transformers import logging, BertTokenizer
import os
import pkg_resources
import nltk
import sys
from symspellpy.symspellpy import SymSpell
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer

class Model(object):

    def _dataset_info(self):
        info = api.info()
        corpora_name = info['corpora'].keys()
        models_name = list(info['models'].keys())
        info = {'corpus': corpora_name, 'models': models_name}
        return info

    def _api_model_save(self, model_name=None):
        '''
        models:
        ['fasttext-wiki-news-subwords-300', 'conceptnet-numberbatch-17-06-300', 'word2vec-ruscorpora-300', 
        'word2vec-google-news-300', 'glove-wiki-gigaword-50', 'glove-wiki-gigaword-100', 'glove-wiki-gigaword-200', 
        'glove-wiki-gigaword-300', 'glove-twitter-25', 'glove-twitter-50', 'glove-twitter-100', 'glove-twitter-200', 
        '__testing_word2vec-matrix-synopsis']
        '''
        save_path = r'D:\path\Wordembedding_models' + r'\{}'.format(model_name)
        model = api.load(model_name)
        model.save(save_path)

    def model_load(self, model_name=None):
        '''
        models:
        ['fasttext-wiki-news-subwords-300', 'conceptnet-numberbatch-17-06-300', 'word2vec-ruscorpora-300', 
        'word2vec-google-news-300', 'glove-wiki-gigaword-50', 'glove-wiki-gigaword-100', 'glove-wiki-gigaword-200', 
        'glove-wiki-gigaword-300', 'glove-twitter-25', 'glove-twitter-50', 'glove-twitter-100', 'glove-twitter-200','glove.42B.300d'
        '__testing_word2vec-matrix-synopsis']
        '''
        load_path = r'D:\path\Wordembedding_models' + r'\{}'.format(model_name)
        model = KeyedVectors.load(load_path)
        return model

    # # only for the model 'glove.42.300d'
    # def _model_created(self,glove_name):
    #     '''
    #     glove_name:
    #     glove.42B.300d
    #     '''
    #     dir_path = r'E:\Guanchen_Pan\MasterThesis\Wordembedding_models'
    #     glove_input_file = dir_path + r'\{}.txt'.format(glove_name)
    #     word2vec_output_file = dir_path + r'\{}.word2vec'.format(glove_name)
    #     glove2word2vec(glove_input_file, word2vec_output_file)

    # def _model_save(self,glove_name):
    #     '''
    #     glove_name:
    #     glove.42B.300d
    #     '''
    #     dir_path = r'E:\Guanchen_Pan\MasterThesis\Wordembedding_models'
    #     load_fpath = dir_path + r'\{}.word2vec'.format(glove_name)
    #     save_fpath = dir_path + r'\{}'.format(glove_name)
    #     model = KeyedVectors.load_word2vec_format(load_fpath, binary=False)
    #     model.save(save_fpath)
    #     return model

class Data_preprocessing(object):
    def __init__(self):
        # Spelling Correction
        self.sym_spell = SymSpell(max_dictionary_edit_distance=0, prefix_length=7)
        dictionary_path = pkg_resources.resource_filename("symspellpy", "frequency_dictionary_en_82_765.txt")
        self.sym_spell.load_dictionary(dictionary_path, term_index=0, count_index=1)

    # get pos
    def _get_wordnet_pos(self, tag):
        if tag.startswith('J'):
            return wordnet.ADJ
        elif tag.startswith('V'):
            return wordnet.VERB
        elif tag.startswith('N'):
            return wordnet.NOUN
        elif tag.startswith('R'):
            return wordnet.ADV
        else:
            return None

    # tokenize
    def tokenize(self, phrase):
        tokens = nltk.word_tokenize(phrase)
        return tokens

    # stemming
    def stemming(self, tokens_list):
        tagged_sent = nltk.pos_tag(tokens_list)
        lemmas = []
        wnl = WordNetLemmatizer()
        for tag in tagged_sent:
            wordnet_pos = self._get_wordnet_pos(tag[1]) or wordnet.NOUN
            lemmas.append(wnl.lemmatize(tag[0], pos=wordnet_pos))
        result = ' '.join(lemmas)
        return result

    def phrase_symspell(self, str):
        # the words without space to phrase
        phrase = self.sym_spell.word_segmentation(str)
        correct_phrase = phrase.corrected_string
        return correct_phrase

    def phrase_preprocessing(self, art='lsp', phrase=None):
        '''
        art('lsp','matrycs')
        '''
        if art == 'lsp':
            temp = self.lsp_attribute(phrase)
        elif art == 'matrycs':
            temp = self.matrycs_attribute(phrase)
        temp = self.tokenize(temp.lower())
        temp = self.stemming(temp)
        return temp

    # lsp_Attributes preprocessing
    def lsp_attribute(self, s):
        s = str(s)
        s = s.replace('_', ' ')
        s = s.replace('-', ' ')
        s = s.replace('(', ' ')
        s = s.replace(')', ' ')
        if ' ' in s:
            return s
        elif len(s) < 2:
            return s
        else:
            for i, element in enumerate(s):
                if i == 0:
                    if s[i] == element.upper() and s[i + 1] == s[i + 1].upper():
                        return s
                elif i > 0 and i < len(s) - 1:
                    if s[i] == element.upper() and (s[i - 1] == s[i - 1].upper() or s[i + 1] == s[i + 1].upper()):
                        return s
            s_list = list(s)
            temp = 0
            for i, element in enumerate(s):
                if i != 0 and element == element.upper():
                    s_list.insert(i + temp, ' ')
                    temp += 1
            s = "".join(s_list)
            return s

    # matrycs_attributes preprocessing
    def matrycs_attribute(self, s):
        s = str(s)
        s = s.replace('_', ' ')
        s = s.replace('-', ' ')
        s = s.replace('(', ' ')
        s = s.replace(')', ' ')
        if s == 'uVIndexMax':
            s = 'uv Index Max'
            return s
        if s == 'consumptionORD':
            s = 'consumption ORD'
            return s
        if ' ' in s:
            return s
        if 'CO2' in s:
            s = s.replace("CO2", 'Cotwo')
        if len(s) < 2:
            return s
        else:
            for i, element in enumerate(s):
                if i == 0:
                    if s[i] == element.upper() and s[i + 1] == s[i + 1].upper():
                        return s
                elif i > 0 and i < len(s) - 1:
                    if s[i] == element.upper() and (s[i - 1] == s[i - 1].upper() or s[i + 1] == s[i + 1].upper()):
                        return s
            s_list = list(s)
            temp = 0
            for i, element in enumerate(s):
                if i != 0 and element == element.upper():
                    s_list.insert(i + temp, ' ')
                    temp += 1

            s = "".join(s_list)
            if 'Cotwo' in s:
                s = s.replace('Cotwo', 'CO2')
            return s

class bert_Energy_tsdae(object):
    def __init__(self, model_name='ontology/EnergyBert'):
        '''
        sbert_energy_stsb
        '''
        self.dp = Data_preprocessing()
        self.tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
        self.model = AutoModel.from_pretrained(model_name, output_hidden_states=True)

    def _embedding(self, sentences):
        tokens = {'input_ids': [], 'attention_mask': []}
        for sent in sentences:
            encoded_dict = self.tokenizer.encode_plus(
                sent,
                add_special_tokens=True,
                max_length=20,
                truncation=True,
                padding='max_length',
                return_attention_mask=True,
                return_tensors='pt'
            )
            tokens['input_ids'].append(encoded_dict['input_ids'])
            tokens['attention_mask'].append(encoded_dict['attention_mask'])
        tokens['input_ids'] = torch.cat(tokens['input_ids'], dim=0)
        tokens['attention_mask'] = torch.cat(tokens['attention_mask'], dim=0)
        self.model.eval()
        with torch.no_grad():
            outputs = self.model(**tokens)
        embeddings = outputs.last_hidden_state
        attention = tokens['attention_mask']
        mask = attention.unsqueeze(-1).expand(embeddings.shape).float()
        mask_embeddings = embeddings * mask
        summed = torch.sum(mask_embeddings, 1)
        counts = torch.clamp(mask.sum(1), min=1e-9)
        mean_pooled = summed / counts
        return mean_pooled

    def _(self, str_a=None, str_b=None):
        if str_b == None:
            return 0
        str_a = self.dp.lsp_attribute(str_a)  # lsp data
        str_b = self.dp.matrycs_attribute(str_b)  # matrycs data
        phrases = [str_a.lower(), str_b.lower()]
        embedding = self._embedding(phrases)
        s = torch.cosine_similarity(embedding[0], embedding[1], dim=0)
        return round(float(s), 3)

    def sim(self, sentences_a=[], sentences_b=[]):
        X = []
        for a in sentences_a:
            str_a = self.dp.lsp_attribute(a)  # lsp data

            X.append(str_a.lower())
        Y = []
        for b in sentences_b:
            str_b = self.dp.matrycs_attribute(b)  # matrycs
            Y.append(str_b.lower())
        embedding1 = self._embedding(X)
        embedding2 = self._embedding(Y)
        cosine_scores = []
        for i in embedding1:
            temp = []
            for j in embedding2:
                s = torch.cosine_similarity(i, j, dim=0)
                temp.append(s)
            cosine_scores.append(temp)
        cosine_scores = torch.tensor(cosine_scores)
        return cosine_scores



if __name__ == '__main__':
    # a = ['The cat sits outside',
    #          'A man is playing guitar',
    #          'The new movie is awesome']
    # b = ['The dog plays in the garden',
    #           'A woman watches TV',
    #           'The new movie is so great']
    a = ['address',
         'alternateName',
         'areaServed',
         'batteryLevel',
         'category',
         'configuration',
         'controlledAsset',
         'controlledProperty',
         'dataProvider',
         'dateCreated',
         'dateFirstUsed',
         'dateInstalled',
         'dateLastCalibration',
         'dateLastValueReported',
         'dateManufactured',
         'dateModified',
         'dateObserved',
         'depth',
         'description',
         'deviceState',
         'direction',
         'distance',
         'dstAware',
         'firmwareVersion',
         'hardwareVersion',
         'id',
         'ipAddress',
         'location',
         'macAddress',
         'mcc',
         'mnc',
         'name',
         'osVersion',
         'owner',
         'provider',
         'refDeviceModel',
         'relativePosition',
         'rssi',
         'seeAlso',
         'serialNumber',
         'softwareVersion',
         'source',
         'supportedProtocol',
         'type',
         'value',
         'energySource',
         'UNIT',
         'sampleInterval',
         'Heat exchanger'
         ]
    b = ['DATE', 'TIMESTAMP', 'LOCATION', 'VALUE', 'ENERGY_SOURCE', 'UNIT_OF_MEASURE', 'Measure', 'INTERVAL'
         ]

    bert = bert_Energy_tsdae(model_name='bert-base-uncased')
    score = bert.sim(sentences_a=a, sentences_b=b)
    print(score)
    pass
