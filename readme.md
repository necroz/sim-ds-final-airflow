# 🏠 Delivery ML Pipeline

**Пример ML-пайплайна для предсказания срока доставки на основе обученной catboost модели с использованием Apache Airflow**

**Шаги** Получение данных из базы, обработка в пандас, предсказание.
**Запуск** Каждые 15 минут по расписанию.

---

## 🚀 Быстрый старт

> ⚠️ Требования:
>
> - Docker + Docker Compose
> - Python 3.9+
> - Git

---

## 🔧 1. Подготовка окружения

### 🐧 Linux / 🍎 macOS

```bash
# 1. Создаём .env файл с UID пользователя (нужно для прав в Docker)
echo "AIRFLOW_UID=$(id -u)" > .env

# 2. Создаём виртуальное окружение
python3 -m venv venv

# 3. Активируем окружение
source venv/bin/activate

# 4. Устанавливаем зависимости
pip install -r requirements.txt
```

---

## 🐳 2. Запуск Airflow для батч-скоринга

### Инициализация Airflow (один раз)

```bash
docker compose up aiflow-init
```

### Запуск всех сервисов

```bash
docker compose up -d
```

### Проверка статуса

```bash
docker compose ps
```

---

## 🌐 4. Работа с Airflow UI

- **Airflow UI**: [http://localhost:8080](http://localhost:8080)
- **Логин**: `airflow`
- **Пароль**: `airflow`
- **DAG**: `sim-ds-basic-pipeline`

---

## 📁 Структура проекта: Airflow на примере

```
airflow-docker/
├── dags/                     # Airflow DAG
│   └── delivery_scoring.py  # Основной DAG для батч-скоринга
├── data/                     # Данные (volume для Docker)
│   ├── input/               # Результаты выборки
│   └── output/              # Результаты обработки и предсказаний
├── models/                   # ML модель (предобучена)
├── logs/                     # Логи Airflow для отладки
├── plugins/                  # Кастомные плагины Airflow
├── config/                   # Конфигурационные файлы
├── docker-compose.yaml      # Конфигурация всех сервисов
└── README.md
```
