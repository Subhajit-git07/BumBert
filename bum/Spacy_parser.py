import pandas as pd
import numpy as np
import spacy
import logging

logger = logging.getLogger(__name__)

nlp = spacy.load('en_core_web_md',exclude=["tagger", "parser", "senter", "attribute_ruler", "lemmatizer", "ner","tok2vec"])


class Spacyparser():

    def __init__(self):

        self.rankDataFrame =pd.DataFrame()


    '''Function to perform sentance similarity check'''

    def spacy_processing(self,tbReportItem,fsReportItem,threshold):

            self.rankDataFrame['StringFS'] = self.rankDataFrame['String_TB'] = self.rankDataFrame['cosine_threshold'] = ''

            try:
                for _, stringTB in enumerate(tbReportItem):
                    for _,stringFS in enumerate(fsReportItem):

                        doc1 = nlp(stringFS)
                        doc2 = nlp(stringTB)

                        cosine_threshold = (np.dot(doc1.vector, doc2.vector) / (np.linalg.norm(doc1.vector) * np.linalg.norm(doc2.vector)))

                        if cosine_threshold>=threshold:
                            self.rankDataFrame = pd.DataFrame(np.insert(self.rankDataFrame.values,0 , values=[stringTB,stringFS,cosine_threshold], axis=0))

            except Exception as e:
                print("Spacy similarity failing", e)
                logger.info("Spacy similarity failing", e)

            if len(self.rankDataFrame) != 0:
                self.rankDataFrame['Rank'] = self.rankDataFrame.groupby(0)[2].rank(ascending=False)
                self.rankDataFrame = self.rankDataFrame[self.rankDataFrame['Rank']<2].rename(columns={0:'Report_item',1:"Report_item_tb",2:'cosine_threshold'}).sort_values(by='cosine_threshold',ascending= False)

            return self.rankDataFrame


