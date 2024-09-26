# Public Sentiment on Security Cameras

This repository presents a detailed analysis of public sentiment toward security cameras, based on discussions from Suomi24, Finland's largest online forum. The dataset spans 20 years (2000-2020), and the analysis leverages natural language processing techniques for sentiment analysis and trend discovery.

## Data Processing Workflow

1. **Data Acquisition**:  
   The Suomi24 dataset was downloaded from [Korp](https://korp.csc.fi/download/Suomi24/) as zip files, which were then converted into CSV format using `zip_to_csv.py`.

2. **Data Filtering**:  
   Relevant discussions were filtered based on specific phrases, as defined in `filter_suomi24_csv.py`.

3. **Translation**:  
   The filtered Finnish data was translated into English using the Opus model developed by Helsinki University.

4. **Data Cleaning**:  
   During the cleaning process:
   - UTF-8 encoded characters like ‘â€™ were replaced with the appropriate ASCII equivalents (e.g., apostrophes).
   - Duplicate sentences were removed to reduce noise and potential spam.
   - The data from 20 CSV files (representing 20 years) was combined into a single dataset.

5. **Text Preprocessing**:  
   Several preprocessing steps were applied to the dataset:
   - Conversion to lowercase
   - Removal of numbers, multiple spaces, punctuations, and single/double characters
   - Removal of stopwords
   - Lemmatization of words  
   The processed text was saved in a new column named `processed_text`.

## Analysis and Visualization

The `Trends.ipynb` notebook includes the following analyses:
- **Monthly Trends**: A visualization of the discussion trends over time.
- **Word Cloud**: A graphical representation of the most frequently discussed topics.
- **Heatmap**: A heatmap showcasing the intensity of discussions over time.
- **Keyword Frequencies**: The frequency of key terms across the dataset.
- **Co-occurrence Heatmap**: A heatmap visualizing the relationships between the keywords.

## Hypothesis Creation and Sentiment Analysis

We formulated several hypotheses to explore specific aspects of public sentiment related to security cameras:
- **Embeddings**: The rows of data were embedded using word embeddings and compared to the hypothesis embeddings using cosine similarity. This allowed for the assignment of each row to a corresponding hypothesis.
- **Sentiment Analysis**: The SentiStrength model was used for mining sentiment, which helped categorize public sentiment into positive, negative, and neutral categories.