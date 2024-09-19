"# Public-Sentiment-on-Security-Cameras" 

downloaded suomi24 data from: https://korp.csc.fi/download/Suomi24/ converted data from zip to CSV using: zip_to_csv.py

then filtered data using filter_suomi24_csv.py by phrases mentioned in the py file

then I used the model opus model given by Helsinki University to translate Finnish data into English

then I cleaned data and then combined the 20 csvs of 20 years of data into 1 csv file
in cleaning I converted some UTF-8 encoded characters like 'â€™ to apostrophe (') and kept only ASCII characters. and removed duplicated sentences to remove noise and spam.

first, I did preprocessing: lowercase, removed numbers, removed multiple spaces, punctuations, single and double characters to dodge utf-8 encoding characters like '&gt' removed stopwords applied word lemmatization saved the text into a new column named processed_text

then I did some analysis in Trends.ipynb notebook, where I displayed;
Monthly Trends in Discussions Over Time
WordCloud
Heatmap of Discussions Over Time
Keyword Frequencies of the keywords mentioned in the notebook cell
Co-occurrence Heatmap of the same Keywords

Then we created a hypothesis and created embedding of the rows and the hypothesis,
used cosine similarity to assign rows to the hypothesis so that these hypotheses can be used as aspects.

then used sentiSentiment for sentiment mining.