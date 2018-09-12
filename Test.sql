--Создаем и стартуем сервер
docker run -itd --name clickhouse-server --ulimit nofile=262144:262144 -p8123:8123/tcp -p9000:9000/tcp -p9009:9009/tcp yandex/clickhouse-server

--Запускаем ранее созданный сервер
docker start clickhouse-server

--Пересоздаем и стартуем клиент
docker run -itd --rm --name clickhouse-client -v d:\projects:/project --link clickhouse-server:clickhouse-server yandex/clickhouse-client --host clickhouse-server

--Подключаемся к кслиенту
docker exec -it clickhouse-client bash

--Создаем таблицы
drop table default.chains;

create table default.chains (
intChainID String,
varGln String,
intDocsCount String,
intArchive String,
varHash String,
intImportant String,
intVisualStatus String,
intLastInDocID String,
intLastOutDocID String,
intLastCommentID String
) engine = Log;

drop table default.chains_docs;

create table default.chains_docs (
intChainID String,
intDocID String
) engine = Log;

--Импортируем файлы
cat /project/clickhouse_docs_export.csv | clickhouse-client --host clickhouse-server --query="INSERT INTO default.chains FORMAT CSV";
cat /project/clickhouse_chains_docs_export.csv | clickhouse-client --host clickhouse-server --query="INSERT INTO default.chains_docs FORMAT CSV";

--select count(1) from default.chains limit 100000;
--select count(1) from default.chains_docs;

--Создаем MergeTree таблицы
--Партиции создаются под КАЖДОЕ значение ключа
--select * from system.parts;
--select * from system.tables;
--drop table default.chains_docs_mt;

create table default.chains_docs_mt
engine = MergeTree partition by partdummy order by intDocID
as
select c.*, '1' partdummy
	from default.chains_docs c
	where 1=0
;

--drop table default.chains_mt;

create table default.chains_mt
engine = MergeTree partition by partdummy order by intChainID
as
select c.*, '1' partdummy
	from default.chains c
	where 1=0
;

select count(*)
	from default.chains_mt
;

--select count(intDocID) from default.chains_docs_mt;
--select count() from default.chains_docs;

insert into default.chains_docs_mt
select c.*, '1' partdummy
	from default.chains_docs c;
	
insert into default.chains_mt
select c.*, '1' partdummy
	from default.chains c;

--Тесты

select * from default.chains_docs_mt;

select * from default.chains_mt;
	
select count(intChainID) cnt
	from default.chains_mt
		any inner join default.chains_docs_mt
			using intChainID;
			
