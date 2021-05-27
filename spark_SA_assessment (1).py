from textblob import TextBlob
from pyspark import SparkConf, SparkContext
import re



def abb_en(line):
    abbreviation_en = {
    'u': 'you',
    'thr': 'there',
    'asap': 'as soon as possible',
    'lv' : 'love',    
    'c' : 'see'
   }
   
    abbrev = ' '.join (abbreviation_en.get(word, word) for word in line.split())
    return (abbrev)

def remove_features(data_str):
   
    url_re = re.compile(r'https?://(www.)?\w+\.\w+(/\w+)*/?')    
    mention_re = re.compile(r'@|#(\w+)')  
    RT_re = re.compile(r'RT(\s+)')
    num_re = re.compile(r'(\d+)')
    
    data_str = str(data_str)
    data_str = RT_re.sub(' ', data_str)  
    data_str = data_str.lower()  
    data_str = url_re.sub(' ', data_str)   
    data_str = mention_re.sub(' ', data_str)  
    data_str = num_re.sub(' ', data_str)
    return data_str

def polarity_check(polarity):
    if polarity > 0:
        polarityvalue = '+ve'
    elif polarity < 0:
        polarityvalue = '-ve'
    else:
        polarityvalue = 'neu'
    
    return polarityvalue
   
  
   

def main(sc,filename):
    rdd = sc.textFile(filename).map(lambda x: x.split(",")).filter(lambda x: len(x) == 8).filter(lambda x: len(x[0]) >1)
    #print(rdd.take(5))                                                                                               
    polarityrdd = rdd.map(lambda x: x[4]).map(lambda x: abb_en(remove_features(x))).map(lambda x: str(x).lower().replace('"','').replace("'",'')).map(lambda x:TextBlob(x).sentiment.polarity).map(lambda x: polarity_check(x))
    #print(polarityrdd.take(5))  
    
    combinerdd =polarityrdd.zip(rdd).map(lambda x:str(x).replace('"','').replace("'",'')).saveAsTextFile("exam2")
    #print(combinerdd.take(5))  
  
   

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[1]").setAppName("analisa")
    sc = SparkContext(conf=conf)
                                    
    filename = "starbucks_v1.csv"
  
    main(sc,filename)
    sc.stop()