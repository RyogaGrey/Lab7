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


# Подключение к RabbitMQ
def get_rabbitmq_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    return connection

# Отправка ссылки в RabbitMQ
def send_to_queue(url):
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE, body=url)
    connection.close()
    logger.info(f"Ссылка добавлена в очередь: {url}")

# Парсинг HTML
async def parse_page(url):
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    title = soup.title.string if soup.title else "Без названия"
                    logger.info(f"Обрабатывается страница: {title} ({url})")

                    # Находим все ссылки
                    for link in soup.find_all("a", href=True):
                        href = link.get("href")
                        abs_url = urljoin(url, href)
                        if is_internal_link(url, abs_url):
                            send_to_queue(abs_url)
                            logger.info(f"Найдена ссылка: {link.string or 'Без описания'} ({abs_url})")
                else:
                    logger.warning(f"Ошибка загрузки страницы: {url} (статус {response.status})")
        except Exception as e:
            logger.error(f"Ошибка при обработке страницы {url}: {e}")

# Проверка, является ли ссылка внутренней
def is_internal_link(base_url, target_url):
    base_netloc = urlparse(base_url).netloc
    target_netloc = urlparse(target_url).netloc
    return base_netloc == target_netloc

# Основной метод
async def main(url):
    await parse_page(url)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        logger.error("Использование: python producer.py <URL>")
        sys.exit(1)

    input_url = sys.argv[1]
    try:
        asyncio.run(main(input_url))
    except KeyboardInterrupt:
        logger.info("Producer остановлен пользователем")
