# Лабораторна 2: NoSQL (Redis + MongoDB) проти SQL (PostgreSQL)

Предметна область - та сама, що в лабораторній 1 (електронна комерція з кількома магазинами).
У Redis винесено дані, які змінюються часто, короткоживучі й читаються за ключем:
кошики (`cart:{user}:{store}` - HASH, TTL 7 днів), кеш пропозицій (`offer:{id}` - HASH),
лічильники переглядів за добу (`views:{day}:{store}` - ZSET). Каталог із гнучкими атрибутами - у MongoDB.

## Структура
- `sql/01_schema.sql, 02_indexes.sql` - таблиці PostgreSQL, що відповідають Redis-структурам
- `sql/03_operations.sql` - SQL-еквіваленти всіх операцій
- `bench/` - генератор даних, клієнти (pgwire.py, resp.py), бенчмарк (bench.py), графіки (charts.py)
- `mongo/` - MongoDB: `01_seed.js` (дані), `02_queries.js` (базові запити)
- `results/` - results.json, results.md; `docs/img` - графіки

## Запуск (рекомендовано: Ubuntu у WSL, все в одному середовищі)
```
sudo apt update && sudo apt install -y postgresql redis-server python3 python3-matplotlib
sudo service postgresql start
sudo service redis-server start
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'postgres'"
sudo -u postgres createdb nosql_lab
cd nosql-lab/bench
python3 bench.py setup --pg-password postgres
python3 bench.py run --pg-password postgres --workers 1 4 8
python3 charts.py
```
Для швидкої перевірки додайте `--quick`. `setup` потрібно повторювати перед кожним `run` (тест змінює дані).
Redis використовує логічну БД №5 (очищується при setup). Повний прогін триває ~20 хв.

## MongoDB
```
sudo apt install -y mongodb-org    # або запустіть у Docker: docker run -d -p 27017:27017 mongo
mongosh "mongodb://localhost:27017/shop_catalog" --file mongo/01_seed.js
mongosh "mongodb://localhost:27017/shop_catalog" --file mongo/02_queries.js
```
