-- SQL-еквіваленти операцій, що виконуються в Redis (параметри: :user, :store, :offer, :qty, :day)

-- Redis: HGETALL cart:{user}:{store}
SELECT ci.offer_id, ci.quantity
  FROM carts c JOIN cart_items ci ON ci.cart_id = c.id
 WHERE c.user_id = :user AND c.store_id = :store;

-- Redis: HGETALL + HMGET offer:{id} name price (по кожній позиції)
SELECT o.id, o.name, o.price, ci.quantity
  FROM carts c JOIN cart_items ci ON ci.cart_id = c.id JOIN offers o ON o.id = ci.offer_id
 WHERE c.user_id = :user AND c.store_id = :store;

-- Redis: HINCRBY cart:{user}:{store} {offer} {qty}; EXPIRE ...
WITH c AS (
    INSERT INTO carts (user_id, store_id) VALUES (:user, :store)
    ON CONFLICT (user_id, store_id) DO UPDATE SET updated_at = now()
    RETURNING id)
INSERT INTO cart_items (cart_id, offer_id, quantity)
SELECT id, :offer, :qty FROM c
ON CONFLICT (cart_id, offer_id) DO UPDATE SET quantity = cart_items.quantity + EXCLUDED.quantity;

-- Redis: DEL cart:{user}:{store}
DELETE FROM cart_items WHERE cart_id = (SELECT id FROM carts WHERE user_id = :user AND store_id = :store);

-- Redis: ZINCRBY views:{day}:{store} 1 {offer}
INSERT INTO offer_views_daily (day, store_id, offer_id, views) VALUES (:day, :store, :offer, 1)
ON CONFLICT (day, store_id, offer_id) DO UPDATE SET views = offer_views_daily.views + 1;

-- Redis: ZREVRANGE views:{day}:{store} 0 9 WITHSCORES
SELECT offer_id, views FROM offer_views_daily WHERE day = :day AND store_id = :store ORDER BY views DESC LIMIT 10;

-- Аналітика (у Redis потребує SCAN усіх кошиків і обчислення на клієнті)
SELECT c.store_id, SUM(ci.quantity * o.price)
  FROM carts c JOIN cart_items ci ON ci.cart_id = c.id JOIN offers o ON o.id = ci.offer_id
 GROUP BY c.store_id ORDER BY c.store_id;
