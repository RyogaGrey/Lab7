import asyncio
import aiohttp
import logging
import pika
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# RabbitMQ настройки
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "links_queue")
QUEUE_TIMEOUT = int(os.getenv("QUEUE_TIMEOUT", 30))

# Подключение к RabbitMQ
def get_rabbitmq_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    return connection

# Парсинг HTML
async def parse_page(url):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    title = soup.title.string if soup.title else "Без названия"
                    logger.info(f"Обрабатывается страница: {title} ({url})")

                    # Находим все ссылки и медиафайлы
                    for tag in soup.find_all(["a", "img", "video", "audio", "source"], href=True):
                        href = tag.get("href")
                        abs_url = urljoin(url, href)
                        if is_internal_link(url, abs_url):
                            send_to_queue(abs_url)
                            logger.info(f"Найдена ссылка: {tag.string or 'Медиа'} ({abs_url})")
                else:
                    logger.warning(f"Ошибка загрузки страницы: {url} (статус {response.status})")
        except Exception as e:
            logger.error(f"Ошибка при обработке страницы {url}: {e}")

# Проверка внутренней ссылки
def is_internal_link(base_url, target_url):
    base_netloc = urlparse(base_url).netloc
    target_netloc = urlparse(target_url).netloc
    return base_netloc == target_netloc

# Обработка сообщений из RabbitMQ
def process_message(ch, method, properties, body):
    url = body.decode()
    logger.info(f"Получена ссылка из очереди: {url}")
    asyncio.run(parse_page(url))
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Основной метод консюмера
def start_consumer():
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    logger.info("Консюмер ожидает сообщения...")

    try:
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=process_message)

        while True:
            if channel.is_closed or channel.is_closing:
                break
            channel.connection.process_data_events(time_limit=QUEUE_TIMEOUT)
            if channel.is_closed:
                logger.info("Очередь пуста, завершение работы.")
                break
    except KeyboardInterrupt:
        logger.info("Консюмер остановлен пользователем")
    finally:
        connection.close()

if __name__ == "__main__":
    start_consumer()
