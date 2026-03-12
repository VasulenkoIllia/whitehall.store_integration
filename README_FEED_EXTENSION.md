# Розширення Імпорту Постачальників: Feed + Mapping

## Мета
Додати до поточного імпорту (Google Sheets) підтримку фідів від різних постачальників з автоматичним виявленням полів, ручним мапінгом і щоденним оновленням.

## Бізнес-логіка
1. Користувач додає джерело типу `feed` з URL.
2. Система читає фід і визначає доступні поля.
3. Користувач мапить поля фіда на наші системні поля.
4. Мапінг зберігається в базі даних для конкретного `source_id`.
5. Щоденний pipeline читає фід, застосовує мапінг і завантажує дані в `products_raw`.

## Системні поля для мапінгу
Обов'язкові:
- `article`
- `price`
- `quantity`

Опціональні:
- `size`
- `extra`

## Формати feed (MVP)
- XML (YML/RSS-подібні прайси)
- CSV
- JSON

## Потік обробки (MVP)
1. `Discover fields`
- Завантажити feed за `source_url`.
- Визначити формат.
- Витягти перші N товарів (наприклад 100).
- Зібрати унікальні поля (для XML/JSON через flatten path, для CSV через header names).
- Повернути перелік полів + sample values.

2. `Mapping`
- Користувач в UI обирає source типу `feed`.
- Система показує знайдені поля.
- Користувач мапить їх до `article/price/quantity/size/extra`.
- Збереження у `column_mappings` з `source_id`.

3. `Import`
- При запуску `import_source`/`import_supplier`/`import_all`:
- Якщо `source_type = feed`, читати feed-парсером.
- Для кожного товару застосовувати mapping.
- Валідовувати і зберігати в `products_raw`.
- Логувати skip-причини (аналогічно Google Sheets).

4. `Daily update`
- Використати існуючий scheduler (`update_pipeline`).
- Feed джерела повинні імпортуватися в тому ж циклі, що і Google Sheets.

## Зміни в backend
1. `src/jobs/runners.js`
- Замінити жорстку перевірку `google_sheet` на router по `source_type`.
- Додати handler для `feed`.

2. `src/services/importService.js`
- Додати `importFeedSource({ source, supplierId, jobId, mappingOverride, mappingMeta })`.
- Винести спільну логіку нормалізації/валідації рядків у reusable helper.

3. Новий сервіс парсингу, наприклад `src/services/feedService.js`
- `detectFeedFormat(url, contentType, bodyPreview)`
- `discoverFeedFields(sourceUrl, options)`
- `readFeedItems(sourceUrl, options)`
- `extractByPath(item, path)`

4. `src/routes/admin.js`
- Додати endpoint для discover полів feed (наприклад `/feed-fields?sourceId=...`).
- Розширити `/source-preview` для `feed` або додати окремий `/feed-preview`.

## Зміни в UI (admin)
1. Джерела
- Додати можливість вибору `source_type = feed`.
- Для feed змінити підпис поля URL на універсальний (`Feed URL`).

2. Mapping
- Додати кнопку `Зчитати поля feed`.
- Показувати список знайдених полів і sample values.
- Дати зберегти mapping так само, як для Google Sheets.

3. Валідація
- Блокувати збереження mapping без `article/price/quantity`.

## Зберігання mapping
Використати існуючу таблицю `column_mappings`:
- `supplier_id`
- `source_id`
- `mapping`
- `header_row` (для feed може бути `null` або `0`)
- `mapping_meta` (рекомендовано зберігати):
  - `source_id`
  - `format` (`xml|csv|json`)
  - `field_paths` (якщо потрібно для debug)
  - `discovered_at`

## Правила якості даних
- Порожній або відсутній `article` -> skip.
- `price <= 0` або не число -> skip.
- `quantity <= 0` -> skip.
- `quantity` за замовчуванням: `1`, якщо поле відсутнє і мапінгом не задано інше.
- Детальні skip-статистики логувати в `logs`.

## Обробка помилок
- Недоступний URL, timeout, 403/404 -> помилка джоби з деталями.
- Невідомий формат feed -> `unsupported feed format`.
- Зміни схеми (поле з мапінгу зникло) -> `mapping validation failed`, вимога remap.

## Безпека і стабільність
- Таймаути на HTTP-запити.
- Ліміти на розмір відповіді.
- Retry з backoff для тимчасових помилок.
- Обмеження на кількість оброблених записів за один прохід (batch/chunk).

## Етапи реалізації
1. Backend parser + discover endpoint.
2. Feed import handler у job runners.
3. UI для вибору типу `feed` і discover/mapping.
4. Логування, edge-cases, стабілізація.
5. Тестовий прогін на реальних фідах постачальників.

## Оцінка
- MVP: 3-5 робочих днів.
- Production-hardening: 5-8 робочих днів (залежно від різноманіття feed форматів і якості джерел).

## Критерії приймання (DoD)
- Можна створити source типу `feed` і зчитати поля.
- Можна зберегти mapping для feed по `source_id`.
- `import_source`, `import_supplier`, `import_all` обробляють feed.
- Щоденний `update_pipeline` включає feed-джерела.
- У логах видно причини пропусків і помилки схеми.
