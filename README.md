

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
yarn jar haein147-0.0.1-SNAPSHOT-executable.jar io.github.haein147.counter.xmlParsing \
/user/mentee/input/kowiki-20190101-pages-meta-current.xml \
/user/mentee/haein/xmlparsing
```

<div>
  <img width="50%" src="https://user-images.githubusercontent.com/43582223/52904613-bcb23680-3271-11e9-97c1-7cddf9faa6e0.png"></img>
 <img  width="50%" src="https://user-images.githubusercontent.com/43582223/52904938-06048500-3276-11e9-853c-57a1a2887339.png"></img>
</div>

<hr />


## 2. from_id와 to_id로 조인해주기 위해 정제
- from_id들이 있는 tsv파일을 dump 받는다.
- dump 받은 파일을 MYSQL에 넣는다.
- pl_id 와 pl_title, namepace 만 뽑아낸다.
- aws s3에 업로드
```
wget https://dumps.wikimedia.org/kowiki/20190101/kowiki-20190101-pagelinks.sql.gz
gunzip kowiki-20190101-pagelinks.sql.gz
mysql -uroot -pwikipedia WIKIPEDIA < kowiki-20190101-pagelinks.sql 
mysql -N -uroot -pwikipedia -e \
"SELECT pl_from, pl_title, pl_namespace FROM pagelinks;" WIKIPEDIA > pagelinks.tsv 
aws s3 cp pagelinks.tsv s3://encore-s3/
```

- tsv 파일의 title 앞에 namespace가 있는 것을 빼준다. 
- namespace 넘버 타이틀
```
yum install jq
curl https://dumps.wikimedia.org/kowiki/20190120/kowiki-20190120-siteinfo-namespaces.json.gz \
|zcat |jq -r '.query.namespaces | to_entries[] | .key +"\t"+ .value["*"]' 
```
<div>
<img  width="50%" src="https://user-images.githubusercontent.com/43582223/52904148-3bf03c00-326b-11e9-91c3-e5518777ec69.png"></img>
</div>
```
yarn jar haein147-0.0.1-SNAPSHOT-executable.jar io.github.haein147.properties.setNameSpace \
/user/mentee/input/pagelinks.tsv \
/user/mentee/haein/setnamespce
```


## 3. redirect 되는 페이지들을 정제한다. 
- redirect.tsv dump를 받아 id를 키로 잡고 Join 해준다.
```
wget https://dumps.wikimedia.org/kowiki/20190101/kowiki-20190101-redirect.sql.gz
gunzip kowiki-20190101-redirect.sql.gz
```
- pagelinks.tsv와 함께 redirect.tsv도 namespace가 0인것만 가져온다.
```
yarn jar haein147-0.0.1-SNAPSHOT-executable.jar io.github.haein147.properties.setNameSpace \
/user/mentee/input/redirect.tsv \
/user/mentee/haein/setnamespce_redirect 
```
<img src="https://user-images.githubusercontent.com/43582223/52905111-b2e00180-3278-11e9-872a-0dd6c70a59e5.png"></img>
