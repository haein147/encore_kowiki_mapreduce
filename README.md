# PageRank 알고리즘 적용을 위한 Mapreduce
### wikipedia 데이터를 알고리즘을 적용하기 위해 정제하고 elasticsearch 엔진에 넣는다

## 1. XML page 데이터를 정제
```
wget https://dumps.wikimedia.org/kowiki/20190101/kowiki-20190101-pages-meta-current.xml.bz2
bunzip kowiki-20190101-pages-meta-current.xml.bz2
```
