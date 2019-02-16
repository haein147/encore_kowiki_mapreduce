# PageRank 알고리즘 적용을 위한 Mapreduce
### wikipedia 데이터를 알고리즘을 적용하기 위해 정제하고 elasticsearch 엔진에 넣는다

## 1. XML page 데이터를 정제
```
wget https://dumps.wikimedia.org/kowiki/20190101/kowiki-20190101-pages-meta-current.xml.bz2
bunzip kowiki-20190101-pages-meta-current.xml.bz2
```
<hr />

- xml을 page단위로 delimiter를 정해준다.
```
conf.set("textinputformat.record.delimiter", "</page>");
```
- ns : namespace 가 0 인 것들만 파싱한다.
- reduce 의 output [to_id, title]

```
mvn clean package
cd target
yarn jar haein147-0.0.1-SNAPSHOT-executable.jar io.github.haein147.counter.xmlParsing /user/mentee/input/kowiki-20190101-pages-meta-current.xml /user/mentee/haein/xmlparsing
```
