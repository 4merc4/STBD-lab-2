// Базові запити MongoDB (CRUD, фільтри, індекси, агрегація).
// Запуск:  mongosh "mongodb://localhost:27017/shop_catalog" --file 02_queries.js
db = db.getSiblingDB("shop_catalog");

print("--- 1. READ: пошук за полем та проєкція");
printjson(db.products.find({ category: "smartphones" }, { name: 1, price: 1 }).toArray());

print("--- 2. READ: фільтр за вкладеним атрибутом і оператором порівняння");
printjson(db.products.find({ "attributes.ram_gb": { $gte: 8 } }, { name: 1, "attributes.ram_gb": 1 }).toArray());

print("--- 3. READ: пошук у масиві тегів, сортування, ліміт");
printjson(db.products.find({ tags: "5g" }).sort({ price: -1 }).limit(2).toArray());

print("--- 4. READ: комбінація умов $and/$or");
printjson(db.products.find({ $or: [ { category: "books" }, { price: { $lt: 1000 }, in_stock: true } ] }, { name: 1 }).toArray());

print("--- 5. UPDATE: змінити ціну одного документа");
printjson(db.products.updateOne({ _id: 1 }, { $set: { price: 14999.0 } }));

print("--- 6. UPDATE: додати відгук у вбудований масив ($push) та змінити тег ($addToSet)");
printjson(db.products.updateOne({ _id: 3 },
  { $push: { reviews: { user_id: 15, rating: 4, text: "Якісна бавовна", created: new Date() } },
    $addToSet: { tags: "bestseller" } }));

print("--- 7. UPDATE: масово, лічильник переглядів ($inc) для категорії");
printjson(db.products.updateMany({ category: "smartphones" }, { $inc: { views: 1 } }));

print("--- 8. INDEX: створення індексів і перегляд плану");
db.products.createIndex({ category: 1, price: -1 });
db.products.createIndex({ tags: 1 });
db.products.createIndex({ name: "text" });
printjson(db.products.find({ category: "smartphones" }).sort({ price: -1 }).explain("executionStats").executionStats.totalDocsExamined);

print("--- 9. TEXT SEARCH");
printjson(db.products.find({ $text: { $search: "Смартфон" } }, { name: 1 }).toArray());

print("--- 10. AGGREGATE: середня ціна та кількість за категоріями");
printjson(db.products.aggregate([
  { $group: { _id: "$category", avg_price: { $avg: "$price" }, n: { $sum: 1 } } },
  { $sort: { n: -1 } } ]).toArray());

print("--- 11. AGGREGATE: середній рейтинг через $unwind вбудованих відгуків");
printjson(db.products.aggregate([
  { $unwind: "$reviews" },
  { $group: { _id: "$_id", name: { $first: "$name" }, avg_rating: { $avg: "$reviews.rating" }, n: { $sum: 1 } } },
  { $sort: { avg_rating: -1 } } ]).toArray());

print("--- 12. DELETE");
printjson(db.products.deleteOne({ _id: 5 }));
printjson(db.products.deleteMany({ in_stock: false }));
print("Залишилось документів: " + db.products.countDocuments());
