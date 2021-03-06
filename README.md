

# PageRank 알고리즘 적용을 위한 Mapreduce

## 클래스 설명 
1. wikipedia 데이터를 알고리즘을 적용하기 위해 정제하고 정제,정제한다.
2. pagerank를 재귀적으로 돌리기 위해 변화율이 10% 미만이 되도록 돌려준다. (여기서는 10번 이내로 잡음)
3. elasticsearch 엔진에 넣는다.

## 구축 환경
 - aws light sail로 hadoop cloudera 서버를 구축(비용 절감을 위해)
 - 32GB RAM, 8v CPU, 640GB SSD 4대
 - elasticsearch 의 엔진도 light sail 로 구축
 - 4GB RAM, 2V CPU, 80GB SSD 2대

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


## 2. 정제
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
yarn jar haein147-0.0.1-SNAPSHOT-executable.jar io.github.haein147.properties.setNameSpace\
/user/mentee/input/redirect.tsv \
/user/mentee/haein/setnamespce_redirect
```
<div>
<img width="50%" s<img width="50%" src="https://user-images.githubusercontent.com/43582223/52905111-b2e00180-3278-11e9-872a-0dd6c70a59e5.png"></img>
<img width="50%" s<img width="50%" src="https://user-images.githubusercontent.com/43582223/52905154-78c32f80-3279-11e9-8511-e8ce3fd791bd.png"></img>
</div>

- xml 파일과 tsv 파일을 redirect 의 to_id 와 같으면 빼준다.
```
yarn jar haein147-0.0.1-SNAPSHOT-executable.jar io.github.haein147.redirectRemove.redirectRemoveXml \
/user/mentee/haein/setnamespace \
/user/mentee/haein/setnamespce_redirect_1 \
/user/mentee/haein/redirect_remove_TSV

yarn jar haein147-0.0.1-SNAPSHOT-executable.jar io.github.haein147.redirectRemove.redirectRemoveXml\
/user/mentee/haein/xmlparsing \
/user/mentee/haein/setnamespce_redirect_1 \
/user/mentee/haein/redirect_remove_XML
```

> setnamespace : 40604591
> setnamespace_redirect : 565588
> redirect_remove_TSV : 40038831

> xmlparsing : 1004322
> setnamespace_redirect : 565588
> redirect_remove_XML : 439033

## 4. from_id to_id 를 만들고 pagerank 적용

- from_id to_id 의 쌍으로 나올 수 있도록 조인
```
yarn jar haein147-0.0.1-SNAPSHOT-executable.jar io.github.haein147.Join.ReduceJoin\
/user/mentee/haein/redirect_remove_TSV \
/user/mentee/haein/redirect_remove_XML \
/user/mentee/haein/from_to_Join
```

- from_id 와 to_id의 list들로 뽑아낸다. 
- 하나의 문서가 어떤 문서 안에 포함되어있는지를 점수로 매겨주어야한다.
- fron_id 1.0 {to_id1, to_id2, to_id3.. }

```
yarn jar haein147-0.0.1-SNAPSHOT-executable.jar io.github.haein147.PageRank.sortToPagerank \
/user/mentee/haein/from_to_Join \
/user/mentee/haein/iter00
```

- **pagerank circulate 를 실행(알고리즘 공식을 코드로 구현함)**
- **PageRank of A = 0.15 + 0.85 * ( PageRank(B)/outgoing links(B) + PageRank(…)/outgoing link(…) )**
- run 을 10으로 잡고 진행 -> 점수를 보고 변화율이 10% 미만일때 중지
```
yarn jar haein147-0.0.1-SNAPSHOT-executable.jar io.github.haein147.PageRank.PageRank \
/user/mentee/haein/iter00
```
<img src="https://user-images.githubusercontent.com/43582223/53784738-97077b80-3f59-11e9-9195-5dbb015815b9.png" ></img>

## pagerank 가 적용된 점수들과 다른 피쳐들과 조인해서 elastic search에 서빙

- pageview, editcount, desctiption size를 뽑아낸다.
- rest client인 Jest API를 써서 elasticsearch로 바로 서빙
- elasticsearch에는 **nori 형태소 분석기**를 사용해서 인덱스를 생성
- 인덱스 생성시 새로 스코어링 한 공식을 script score로 적용
<img src="https://user-images.githubusercontent.com/43582223/53785199-09c52680-3f5b-11e9-90da-dae509285788.png" ></img>

```
yarn jar haein147-0.0.1-SNAPSHOT-executable.jar io.github.haein147.Join.descriptionJoin \
/user/mentee/haein/iter10 \
/user/mentee/haein/otherfeatures \ 
/user/mentee/haein/elastic
```

