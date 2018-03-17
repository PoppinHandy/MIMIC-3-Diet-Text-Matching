# -*- coding: utf-8 -*-
"""
Created on Fri Mar 16 14:08:54 2018

@author: Andy Pham
"""

import psycopg2 as pg
import re
import string
import pandas as pd

def tokenizeDietText(query):
    # Remove punctuation
    punct = re.compile('[%s]' % re.escape(string.punctuation))
    
    # Remove stop gap words
    stopWords = ['and', 'an', 'was', 'at', 'is', 'with', 'the', 'were', 'by', 'it', 'its']
    #stopRegex = re.compile(r'\band\b | \bto\b | \ban\b | \bwith\b', flags=re.I | re.X)
    stopRegex = re.compile(r"\b({})\b".format("|".join(stopWords)))
    #print("\b({})\b".format("|".join(stopWords)))
    
    # Will have format [subject_id, hadm_id, text without stop gap or punctuation]
    tokenizedList = []
    for row in query:
        noWhiteSpaceText = re.sub('\s+',' ',row[2])
        noStopGap = re.sub(stopRegex, '', noWhiteSpaceText)
        noPunctuationText = re.sub(punct, '', noStopGap)
        tokenizedList.append([row[0], row[1], noPunctuationText])
    return tokenizedList

def main():
    conn = pg.connect(database="mimic", user="mimic_ro", password="stone birthday floor spring", host="psql-mimic.ri.ucdavis.edu")
    cur = conn.cursor()
    query = """
    set search_path=mimiciii;
    SELECT subject_id, hadm_id, diet
    FROM ap_notnpo_patients
    LIMIT 100;
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    tokenizedList = tokenizeDietText(rows)
    # Call this script.main() to access the list
    return tokenizedList
    
if __name__=="__main__":
    main()
