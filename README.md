# Collocation-Extraction
Distributed System Programming
 
How to run?
1.	Create a jar that is not mentionend in the main class, after creating the jar, change it's name to the name specified in the DEFNS.java file.
2.	Create a S3 bucket with matched name to the project name in the DEFNS. Class
3.	Put in the terminal - java -jar target/CollocationsExtractionJar.jar 0.5 0.2
 0.5 0.2 are the Minimal pmi and the  Relative minimal pmi respectively.
How The Program Works?
    
A collocation is a sequence of words or terms that co-occur more often than would be expected by
chance. The identification of collocations - such as, 'rain deer', 'global warming' ''בית חולים ect.    
We have coded a map-reduce job running on the Amazon Elastic MapReduce service. 
 The collocation criteria will be based on the normalized PMI value:
  *  Minimal pmi: in case the normalized PMI is equal or greater than the given minimal PMI
     value (denoted by minPmi), the given pair of two ordered words is considered to be a collocation.
  * Relative minimal pmi: in case the normalized PMI value, divided by the sum of all
    normalized pmi in the same decade (including those which their normalized PMI is less
    than minPmi), is equal or greater than the given relative minimal PMI value (denoted by
    relMinPmi), the given pair of two ordered words is considered to be a collocation.
We Ran our experiments on the 2-grams Hebrew and English corpus. 
The input is 2-Gram dataset of Google Books Ngrams. 
The output of the program is a list of the collocations for each decade, and there npmi value, ordered by their npmi (descending).
    
 

The Steps:
    1. CalcCw1w2N: calculates N for each decade and C(w1w2) for each bigram per decade, and filters bigrams using a stop-words list.
    2. CalcCw1: calculates C(w1) for each bigram per decade.
    3. CalcCw2: calculates C(w2) for each bigram per decade.
    4. CalcNpmi: calculates npmi value for each bigram per decade.
    5. Sort and filter: filters only bigrams that found as collocations, sorts the filtered bigrams per decade and displays their npmi value, descending..
    
    
