RabbitMQ Message Queue

Программа состоит из двух частей: producer.py и consumer.py.

Producer:
1. Получает URL в качестве аргумента командной строки.
2. Загружает страницу по этому URL и извлекает все внутренние ссылки (ссылки на тот же домен).
3. Отправляет найденные ссылки в очередь RabbitMQ.

Consumer:
1. Получает сообщения (ссылки) из очереди RabbitMQ.
2. Загружает страницы по этим ссылкам и извлекает новые внутренние ссылки.
3. Добавляет найденные ссылки обратно в очередь.

Использование:

1. Настройте файл .env с параметрами подключения к RabbitMQ:
   RABBITMQ_HOST=localhost
   RABBITMQ_PORT=5672
   RABBITMQ_USER=guest
   RABBITMQ_PASSWORD=guest
   RABBITMQ_QUEUE=links_queue
   QUEUE_TIMEOUT=30

2. Убедитесь, что RabbitMQ запущен. Например, можно запустить через Docker:
   docker run -d --hostname my-rabbit --name some-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management

3. Установите необходимые библиотеки:
   pip install aiohttp pika beautifulsoup4 python-dotenv

4. Запустите producer.py, передав URL:
   python producer.py https://example.com

5. Запустите consumer.py для обработки сообщений из очереди:
   python consumer.py

6. Чтобы остановить consumer.py, нажмите Ctrl+C.