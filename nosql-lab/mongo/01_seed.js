// MongoDB: каталог товарів з гнучкими атрибутами та вбудованими відгуками.
// Запуск:  mongosh "mongodb://localhost:27017/shop_catalog" --file 01_seed.js
db = db.getSiblingDB("shop_catalog");
db.products.drop();

db.products.insertMany([
  { _id: 1, store_id: 1, name: "Смартфон Nova X", category: "smartphones", price: 15999.0, in_stock: true,
    tags: ["5g", "oled", "новинка"],
    attributes: { ram_gb: 8, storage_gb: 256, screen_inch: 6.5, color: "black" },
    reviews: [ { user_id: 10, rating: 5, text: "Чудовий екран", created: new Date("2026-10-01") },
               { user_id: 11, rating: 4, text: "Швидкий, але гріється", created: new Date("2026-10-05") } ] },
  { _id: 2, store_id: 1, name: "Смартфон Lite 5", category: "smartphones", price: 6999.0, in_stock: true,
    tags: ["budget", "5g"],
    attributes: { ram_gb: 4, storage_gb: 64, screen_inch: 6.1, color: "blue" },
    reviews: [ { user_id: 12, rating: 3, text: "За ці гроші нормально", created: new Date("2026-10-07") } ] },
  { _id: 3, store_id: 2, name: "Футболка Basic", category: "clothing", price: 599.0, in_stock: true,
    tags: ["cotton", "unisex"],
    attributes: { size: ["S", "M", "L", "XL"], material: "cotton", color: "white" },   // інші атрибути, ніж у телефона
    reviews: [] },
  { _id: 4, store_id: 2, name: "Куртка Winter", category: "clothing", price: 3499.0, in_stock: false,
    tags: ["winter", "waterproof"],
    attributes: { size: ["M", "L"], material: "polyester", color: "green", waterproof: true },
    reviews: [ { user_id: 10, rating: 5, text: "Тепла", created: new Date("2026-09-20") },
               { user_id: 13, rating: 5, text: "Не промокає", created: new Date("2026-09-25") } ] },
  { _id: 5, store_id: 3, name: "Роман «Кобзар»", category: "books", price: 350.0, in_stock: true,
    tags: ["classic"],
    attributes: { author: "Т. Шевченко", pages: 480, language: "uk" },
    reviews: [ { user_id: 14, rating: 5, text: "Класика", created: new Date("2026-08-15") } ] }
]);
print("Вставлено документів: " + db.products.countDocuments());
