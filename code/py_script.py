import pandas as pd

A = pd.read_csv("stopwords.csv")
A[[0]].to_csv("stopwords.csv", index = False)