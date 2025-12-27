# whitehall_store_integration

## Запуск і загальна схема роботи

Сервіс збирає дані з джерел постачальників (Google Sheets/Excel), формує merged/final, готує API‑набір для Horoshop і (за потреби) відправляє його в Horoshop.

### 1) Швидкий запуск через Docker

1. Скопіюй `.env.example` у `.env` і заповни змінні.
2. Запусти:

```bash
docker compose up -d --build
```

За замовчуванням сервіс доступний:
- Адмінка: `http://localhost:3007/admin`
- Read‑only (перегляд): `http://localhost:3007/`

### 2) Локальний запуск без Docker

1. Встанови залежності:

```bash
npm install
npm --prefix admin-ui install
```

2. Підготуй `.env`.
3. Збери адмінку:

```bash
npm run admin:build
```

4. Запусти міграції та сервер:

```bash
npm run migrate
npm start
```

Адмінка буде доступна на `http://localhost:3000/admin`.

### 3) Обов’язкові змінні в `.env`

- `DATABASE_URL`
- `GOOGLE_CLIENT_EMAIL`
- `GOOGLE_PRIVATE_KEY` (рядки через `\n`)
- `HOROSHOP_DOMAIN`, `HOROSHOP_LOGIN`, `HOROSHOP_PASSWORD` (для sync/import)
- `ENABLE_CRON` (для планового запуску)

### 4) Права доступу до Google Sheets

Сервісний акаунт має мати доступ до таблиць. Потрібно пошарити таблицю на email сервіс‑акаунта з `GOOGLE_CLIENT_EMAIL`.

### 5) Як працює процес

Основний пайплайн:
1. **Імпорт**: читає всі джерела (Google Sheets).
2. **Finalize**: застосовує націнки, округлення, дедуплікацію.
3. **Export**: формує API‑набір для Horoshop.
4. **Horoshop import**: відправляє API‑набір у Horoshop.

Усе керується з адмінки:
- `/admin` — повне керування.
- `/` — режим перегляду без редагування.

## Deploy script (deploy-compose.sh)

Скрипт автоматично піднімає Docker Compose і налаштовує Nginx + SSL.

### Що робить скрипт
- Читає `.env` у директорії проекту.
- Перевіряє обов’язкові змінні: `PROJECT_NAME`, `DOMAIN`, `LE_EMAIL`.
- Визначає `EXPOSED_PORT` з `docker-compose.yml` (127.0.0.1:PORT:3000).
- Запускає `docker compose up -d --build`.
- Налаштовує Nginx reverse proxy на `EXPOSED_PORT`.
- Отримує SSL через Let’s Encrypt (certbot) і вмикає redirect на HTTPS.
- Якщо SSL не видався — робить rollback nginx‑конфіга.

### Як запускати

```bash
chmod +x deploy-compose.sh
sudo -E ./deploy-compose.sh
```

### Потрібні умови
- Домен `DOMAIN` має бути прив’язаний до IP сервера.
- Відкриті порти 80/443.
- Docker і nginx можуть бути встановлені скриптом автоматично.
