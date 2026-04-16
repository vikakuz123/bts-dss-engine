# Bitrix24 to Qdrant Sync

Этот проект выгружает `компании` и `сделки` из Bitrix24, превращает их в текстовые документы, создает локальные эмбеддинги через FastEmbed и сохраняет в Qdrant.

## Что делает

- читает компании через `crm.company.list`
- читает сделки через `crm.deal.list`
- собирает нормализованный текст для векторизации
- создает embeddings локально через FastEmbed
- загружает точки в одну коллекцию Qdrant

## Подготовка

1. Создай виртуальное окружение:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

2. Установи зависимости:

```powershell
pip install -r requirements.txt
```

3. Создай `.env` на основе примера:

```powershell
Copy-Item .env.example .env
```

4. Заполни переменные:

- `BITRIX_WEBHOOK_BASE`
- `QDRANT_URL`
- `QDRANT_API_KEY` если нужен
- `QDRANT_COLLECTION`
- `EMBEDDING_MODEL`

## Запуск

```powershell
python .\sync_bitrix_to_qdrant.py
```

## Поиск по данным

После загрузки данных можно искать похожие компании и сделки:

```powershell
python .\search_qdrant.py "Москва"
```

Ограничить количество результатов:

```powershell
python .\search_qdrant.py "Москва" --limit 3
```

Искать только среди компаний:

```powershell
python .\search_qdrant.py "Москва" --entity-type company
```

Искать только среди сделок:

```powershell
python .\search_qdrant.py "тестовая сделка" --entity-type deal
```

## Локальный API

После настройки `.env` можно запустить локальный backend:

```powershell
uvicorn app.main:app --reload
```

Проверка в браузере:

- `http://127.0.0.1:8000/`
- `http://127.0.0.1:8000/health`
- `http://127.0.0.1:8000/health/db`
- `http://127.0.0.1:8000/setup/db` (POST)
- `http://127.0.0.1:8000/health/qdrant`
- `http://127.0.0.1:8000/docs`

## Что попадет в Qdrant

Каждая точка содержит:

- `entity_type`: `company` или `deal`
- `entity_id`: id сущности в Bitrix24
- `title`: название компании или сделки
- `document`: текст, который ушел в embeddings
- `source`: `bitrix24`
- `raw`: исходные поля записи

## GitHub

После заполнения файлов и проверки можно выполнить:

```powershell
git add .
git commit -m "Add Bitrix24 to Qdrant sync"
git push -u origin main
```

## GitHub Actions

В репозитории уже добавлен workflow для автоматической синхронизации:

- ручной запуск через `Actions` -> `Sync Bitrix24 to Qdrant` -> `Run workflow`
- автоматический запуск каждые 6 часов

Перед запуском нужно добавить в GitHub Secrets значения:

- `BITRIX_WEBHOOK_BASE`
- `QDRANT_URL`
- `QDRANT_API_KEY`
- `QDRANT_COLLECTION`
- `EMBEDDING_MODEL`

Добавить их можно в:

- `GitHub` -> `CodeX` -> `Settings` -> `Secrets and variables` -> `Actions`

## Что еще нужно для полной автоматизации

- GitHub token для push или GitHub Actions
- доступный Qdrant сервер
- секреты в `.env` локально или в secrets на сервере/GitHub
- при первом запуске FastEmbed скачает модель эмбеддингов
