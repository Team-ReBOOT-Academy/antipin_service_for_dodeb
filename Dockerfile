# Этап 1: Сборка JAR-файла
FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /app

# Копируем файлы сборщика
COPY pom.xml mvnw ./
COPY .mvn .mvn

# Копируем исходный код и собираем JAR
COPY src ./src
RUN ./mvnw clean package -DskipTests

# Этап 2: Финальный легковесный образ для запуска
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Безопасность: запуск от непривилегированного пользователя
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

# Копируем скомпилированный JAR
COPY --from=builder /app/target/*.jar app.jar

# Порт по умолчанию
EXPOSE 8080

# Переменные окружения по умолчанию
ENV PORT=8080
ENV SERVICE_NAME=antipin_service_for_dodeb
ENV APP_VERSION=1.0.0

# Запуск в exec-форме для корректной обработки сигналов SIGTERM/SIGINT (Graceful Shutdown)
ENTRYPOINT ["java", "-jar", "app.jar"]
