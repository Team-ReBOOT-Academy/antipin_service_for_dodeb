# antipin_service_for_dodeb

Микросервис торговой стратегии для учебной платформы Teacher Trading Platform.

* **Автор:** Антипин Александр Алексеевич
* **Репозиторий:** `antipin_service_for_dodeb`
* **Идентификатор аккаунта (`accountId`):** `antipin-aa`
* **Отображаемое имя (`accountName`):** `Антипин Александр`
* **Имя сервиса (`serviceName`):** `antipin_service_for_dodeb`
* **Версия сервиса:** `1.0.0`
* **Выбранная стратегия:** `rsi-reversal` (RSI Reversal Strategy)
* **Архитектурная документация:** [Проектный документ (docs/project-description.md)](docs/project-description.md)

---

## Описание проекта
Сервис предназначен для автономной алгоритмической обработки рыночных данных, поступающих от платформы преподавателя через шину сообщений Apache Kafka. Сервис анализирует поток закрытых свечей, рассчитывает технические индикаторы и формирует торговые рекомендации (`BUY`, `SELL`, `HOLD`), а также отслеживает состояние внутренних компонентов через эндпоинт проверки здоровья.

---

## Требования к окружению
Для сборки и запуска сервиса необходим один из следующих вариантов:
1. **Вариант 1 (рекомендуемый):** Docker (Docker Engine или Docker Desktop).
2. **Вариант 2:** Java Development Kit (JDK) версии 21 или выше. Предустановка Maven не требуется (используется встроенный Maven Wrapper `./mvnw`).

---

## Переменные окружения
Настройки задаются через переменные окружения (шаблон доступен в файле `.env.example`):

| Переменная | Значение по умолчанию | Описание |
| :--- | :--- | :--- |
| `PORT` | `8080` | Порт встроенного веб-сервера |
| `SERVICE_NAME` | `antipin_service_for_dodeb` | Уникальное имя сервиса в платформе |
| `APP_VERSION` | `1.0.0` | Версия сервиса в формате SemVer |

---

## Сборка и запуск

### 1. Запуск через Docker (без среды разработки)

Сборка Docker-образа:
```bash
docker build -t antipin_service_for_dodeb:1.0.0 .
```

Запуск контейнера:
```bash
docker run -d -p 8080:8080 --name trading-strategy antipin_service_for_dodeb:1.0.0
```

Остановка контейнера (поддерживается Graceful Shutdown):
```bash
docker stop trading-strategy
```

---

### 2. Локальная сборка и запуск через Maven Wrapper

Запуск тестов:
```bash
./mvnw test
```

Сборка исполняемого JAR-файла:
```bash
./mvnw clean package -DskipTests
```

Запуск сервиса:
```bash
java -jar target/antipin_service_for_dodeb-1.0.0.jar
```
*(либо напрямую: `./mvnw spring-boot:run`)*

---

## Проверка состояния (Health Check)

Сервис предоставляет публичный эндпоинт проверки доступности без авторизации:
- **Метод:** `GET`
- **URL:** `http://localhost:8080/health`

### Пример запроса через curl:
```bash
curl -i http://localhost:8080/health
```

### Ожидаемый ответ (HTTP 200 OK):
```json
{
  "status": "UP",
  "service": "antipin_service_for_dodeb",
  "version": "1.0.0",
  "components": {}
}
```
