"""
S3 Folder Watcher — Windows-служба для автоматической загрузки файлов
в Timeweb Cloud S3 хранилище.

Описание: Следит за указанной папкой и загружает новые/изменённые файлы
           в S3-бакет Timeweb Cloud.
"""

import os
import sys
import time
import json
import logging
import hashlib
import threading
import queue
from pathlib import Path
from datetime import datetime

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ─────────────────────────────────────────────
# Путь к файлу конфигурации (рядом с .py/.exe)
# ─────────────────────────────────────────────
if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).parent

CONFIG_PATH = BASE_DIR / "config.json"
STATE_PATH = BASE_DIR / "upload_state.json"
LOG_PATH = BASE_DIR / "s3_watcher.log"

# ─────────────────────────────────────────────
# Значения по умолчанию
# ─────────────────────────────────────────────
DEFAULT_CONFIG = {
    "s3_endpoint": "https://s3.twcstorage.ru",
    "s3_region": "ru-1",
    "s3_access_key": "",
    "s3_secret_key": "",
    "s3_bucket": "",
    "s3_prefix": "",
    "watch_folder": r"C:\WatchFolder",
    "scan_interval_sec": 60,
    "upload_existing_on_start": False,
    "file_extensions": [],
    "ignore_patterns": [".tmp", ".partial", "~$", "Thumbs.db", "desktop.ini"],
    "max_retries": 3,
    "retry_delay_sec": 5,
    "multipart_threshold_mb": 50,
    "log_level": "INFO"
}


# ─────────────────────────────────────────────
# Логгирование
# ─────────────────────────────────────────────
def setup_logging(level_name: str = "INFO"):
    level = getattr(logging, level_name.upper(), logging.INFO)

    logger = logging.getLogger("S3Watcher")
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # Файл
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setLevel(level)

    # Консоль (для отладки)
    ch = logging.StreamHandler()
    ch.setLevel(level)

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# ─────────────────────────────────────────────
# Конфигурация
# ─────────────────────────────────────────────
def load_config() -> dict:
    """Загружает конфигурацию из JSON-файла. Если файла нет — создаёт шаблон."""
    if not CONFIG_PATH.exists():
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, indent=4, ensure_ascii=False)
        print(f"[!] Создан файл конфигурации: {CONFIG_PATH}")
        print("    Отредактируйте его и запустите программу снова.")
        sys.exit(1)

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    # Дополняем недостающие ключи значениями по умолчанию
    for key, val in DEFAULT_CONFIG.items():
        if key not in cfg:
            cfg[key] = val

    # Поддержка переменных окружения (приоритет над config.json)
    cfg["s3_access_key"] = os.environ.get("S3_ACCESS_KEY", cfg["s3_access_key"])
    cfg["s3_secret_key"] = os.environ.get("S3_SECRET_KEY", cfg["s3_secret_key"])

    return cfg


# ─────────────────────────────────────────────
# Общая функция фильтрации файлов
# ─────────────────────────────────────────────
def should_ignore(filepath: str, cfg: dict) -> bool:
    """Проверяет, нужно ли игнорировать файл."""
    basename = os.path.basename(filepath)
    for pattern in cfg.get("ignore_patterns", []):
        if pattern in basename:
            return True

    extensions = cfg.get("file_extensions", [])
    if extensions:
        _, ext = os.path.splitext(filepath)
        if ext.lower() not in [e.lower() for e in extensions]:
            return True

    return False


# ─────────────────────────────────────────────
# Состояние загрузок (какие файлы уже загружены)
# ─────────────────────────────────────────────
class UploadState:
    """Хранит MD5-хеши загруженных файлов для предотвращения дублей."""

    def __init__(self, path: Path):
        self.path = path
        self.data: dict = {}
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
            except (json.JSONDecodeError, IOError):
                self.data = {}

    def save(self):
        """Атомарная запись состояния через временный файл."""
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        tmp.replace(self.path)

    @staticmethod
    def file_hash(filepath: str) -> str:
        """Вычисляет MD5-хеш файла."""
        h = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    def needs_upload(self, filepath: str) -> tuple:
        """Проверяет, изменился ли файл. Возвращает (needs_upload, hash)."""
        try:
            current_hash = self.file_hash(filepath)
        except (IOError, OSError):
            return False, None
        return self.data.get(filepath) != current_hash, current_hash

    def mark_uploaded(self, filepath: str, file_hash: str):
        """Помечает файл как загруженный."""
        with self._lock:
            self.data[filepath] = file_hash
            self.save()


# ─────────────────────────────────────────────
# S3-клиент (обёртка над boto3)
# ─────────────────────────────────────────────
class S3Uploader:
    def __init__(self, cfg: dict, logger: logging.Logger):
        self.cfg = cfg
        self.log = logger
        self.bucket = cfg["s3_bucket"]
        self.prefix = cfg.get("s3_prefix", "").strip("/")

        self.client = boto3.client(
            "s3",
            endpoint_url=cfg["s3_endpoint"],
            region_name=cfg["s3_region"],
            aws_access_key_id=cfg["s3_access_key"],
            aws_secret_access_key=cfg["s3_secret_key"],
        )

        # Настройка multipart
        threshold = cfg.get("multipart_threshold_mb", 50) * 1024 * 1024
        self.transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=threshold,
            max_concurrency=4,
        )

    def test_connection(self) -> bool:
        """Проверяет подключение к S3."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
            self.log.info(f"Подключение к бакету '{self.bucket}' — OK")
            return True
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "404":
                self.log.error(f"Бакет '{self.bucket}' не найден!")
            elif code == "403":
                self.log.error(f"Доступ к бакету '{self.bucket}' запрещён. Проверьте ключи.")
            else:
                self.log.error(f"Ошибка подключения к бакету: {e}")
            return False
        except EndpointConnectionError:
            self.log.error(f"Не удалось подключиться к {self.cfg['s3_endpoint']}")
            return False

    def upload_file(self, local_path: str, watch_folder: str) -> bool:
        """Загружает файл в S3, сохраняя структуру папок."""
        rel_path = os.path.relpath(local_path, watch_folder)
        # Нормализуем путь для S3 (всегда прямые слэши)
        s3_key = rel_path.replace("\\", "/")
        if self.prefix:
            s3_key = f"{self.prefix}/{s3_key}"

        retries = self.cfg.get("max_retries", 3)
        delay = self.cfg.get("retry_delay_sec", 5)

        for attempt in range(1, retries + 1):
            try:
                file_size = os.path.getsize(local_path)
                size_str = self._format_size(file_size)

                self.log.info(
                    f"Загрузка: {rel_path} ({size_str}) → s3://{self.bucket}/{s3_key}"
                    + (f"  [попытка {attempt}/{retries}]" if attempt > 1 else "")
                )

                self.client.upload_file(
                    local_path,
                    self.bucket,
                    s3_key,
                    Config=self.transfer_config,
                )

                self.log.info(f"  ✓ Загружен: {s3_key}")
                return True

            except ClientError as e:
                self.log.warning(f"  ✗ Ошибка S3: {e}")
            except (IOError, OSError) as e:
                self.log.warning(f"  ✗ Ошибка чтения файла: {e}")
            except Exception as e:
                self.log.warning(f"  ✗ Неизвестная ошибка: {e}")

            if attempt < retries:
                self.log.info(f"  Повтор через {delay} сек...")
                time.sleep(delay)

        self.log.error(f"  ✗ Не удалось загрузить {rel_path} после {retries} попыток")
        return False

    @staticmethod
    def _format_size(size: int) -> str:
        for unit in ("Б", "КБ", "МБ", "ГБ"):
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} ТБ"


# ─────────────────────────────────────────────
# Обработчик событий файловой системы
# ─────────────────────────────────────────────
class FolderEventHandler(FileSystemEventHandler):
    def __init__(self, uploader: S3Uploader, state: UploadState,
                 cfg: dict, logger: logging.Logger):
        super().__init__()
        self.uploader = uploader
        self.state = state
        self.cfg = cfg
        self.log = logger
        self._debounce_sec = 2.0
        # Очередь + один рабочий поток вместо множества Timer-ов
        self._pending: dict[str, float] = {}
        self._pending_lock = threading.Lock()
        self._queue: queue.Queue = queue.Queue()
        self._worker = threading.Thread(target=self._upload_worker, daemon=True)
        self._worker.start()

    def _upload_worker(self):
        """Единый рабочий поток для обработки файлов с debounce."""
        while True:
            # Собираем новые события из очереди
            try:
                path = self._queue.get(timeout=self._debounce_sec / 2)
                with self._pending_lock:
                    self._pending[path] = time.time()
            except queue.Empty:
                pass

            # Обрабатываем файлы, которые «устоялись» (нет новых событий)
            now = time.time()
            with self._pending_lock:
                ready = [
                    p for p, t in self._pending.items()
                    if now - t >= self._debounce_sec
                ]
                for path in ready:
                    self._pending.pop(path)

            for path in ready:
                self._try_upload(path)

    def _process_file(self, path: str):
        """Добавляет файл в очередь на загрузку."""
        if not os.path.isfile(path):
            return
        if should_ignore(path, self.cfg):
            return
        self._queue.put(path)

    def _try_upload(self, path: str):
        """Загружает файл, если он изменился."""
        if not os.path.isfile(path):
            return

        # Проверяем, что файл не заблокирован (дозаписывается)
        if not self._is_file_ready(path):
            self.log.debug(f"Файл ещё занят: {path}, отложим...")
            time.sleep(1)
            if not self._is_file_ready(path):
                return

        needs, file_hash = self.state.needs_upload(path)
        if not needs:
            return

        watch_folder = self.cfg["watch_folder"]
        success = self.uploader.upload_file(path, watch_folder)
        if success:
            self.state.mark_uploaded(path, file_hash)

    @staticmethod
    def _is_file_ready(path: str) -> bool:
        """Проверяет, что файл доступен для чтения (не заблокирован)."""
        try:
            with open(path, "rb"):
                return True
        except (IOError, PermissionError):
            return False

    def on_created(self, event):
        if not event.is_directory:
            self.log.debug(f"Новый файл: {event.src_path}")
            self._process_file(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self.log.debug(f"Изменён файл: {event.src_path}")
            self._process_file(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self.log.debug(f"Перемещён: {event.src_path} → {event.dest_path}")
            self._process_file(event.dest_path)


# ─────────────────────────────────────────────
# Сканер существующих файлов
# ─────────────────────────────────────────────
def scan_existing_files(watch_folder: str, uploader: S3Uploader,
                        state: UploadState, cfg: dict, logger: logging.Logger):
    """Сканирует папку и загружает файлы, которые ещё не были загружены."""
    logger.info(f"Сканирование существующих файлов в: {watch_folder}")
    count = 0

    for root, dirs, files in os.walk(watch_folder):
        for filename in files:
            filepath = os.path.join(root, filename)

            if should_ignore(filepath, cfg):
                continue

            needs, file_hash = state.needs_upload(filepath)
            if needs:
                if uploader.upload_file(filepath, watch_folder):
                    state.mark_uploaded(filepath, file_hash)
                    count += 1

    logger.info(f"Сканирование завершено. Загружено файлов: {count}")


# ─────────────────────────────────────────────
# Общая логика инициализации watcher
# ─────────────────────────────────────────────
def create_watcher(cfg: dict, logger: logging.Logger) -> Observer:
    """Создаёт и возвращает настроенный Observer, готовый к запуску."""
    watch_folder = cfg["watch_folder"]
    if not os.path.isdir(watch_folder):
        logger.info(f"Создание папки для наблюдения: {watch_folder}")
        os.makedirs(watch_folder, exist_ok=True)

    uploader = S3Uploader(cfg, logger)
    if not uploader.test_connection():
        raise RuntimeError("Не удалось подключиться к S3. Проверьте config.json")

    state = UploadState(STATE_PATH)

    if cfg.get("upload_existing_on_start", False):
        scan_existing_files(watch_folder, uploader, state, cfg, logger)

    handler = FolderEventHandler(uploader, state, cfg, logger)
    observer = Observer()
    observer.schedule(handler, watch_folder, recursive=True)

    return observer, uploader, state


# ─────────────────────────────────────────────
# Периодическое сканирование (подстраховка)
# ─────────────────────────────────────────────
def periodic_scan(interval: int, watch_folder: str, uploader: S3Uploader,
                  state: UploadState, cfg: dict, logger: logging.Logger):
    """Периодически сканирует папку для подстраховки (watchdog может пропустить события)."""
    while True:
        time.sleep(interval)
        try:
            scan_existing_files(watch_folder, uploader, state, cfg, logger)
        except Exception as e:
            logger.warning(f"Ошибка при периодическом сканировании: {e}")


# ─────────────────────────────────────────────
# Основной цикл работы (не-сервисный режим)
# ─────────────────────────────────────────────
def run_watcher(cfg: dict):
    """Запускает наблюдатель за папкой."""
    logger = setup_logging(cfg.get("log_level", "INFO"))

    logger.info("=" * 60)
    logger.info("  S3 Folder Watcher — Timeweb Cloud")
    logger.info(f"  Endpoint:  {cfg['s3_endpoint']}")
    logger.info(f"  Bucket:    {cfg['s3_bucket']}")
    logger.info(f"  Папка:     {cfg['watch_folder']}")
    logger.info("=" * 60)

    observer, uploader, state = create_watcher(cfg, logger)
    observer.start()

    # Периодическое сканирование (подстраховка watchdog)
    scan_interval = cfg.get("scan_interval_sec", 0)
    if scan_interval > 0:
        scanner = threading.Thread(
            target=periodic_scan,
            args=(scan_interval, cfg["watch_folder"], uploader, state, cfg, logger),
            daemon=True,
        )
        scanner.start()
        logger.info(f"Периодическое сканирование каждые {scan_interval} сек.")

    logger.info("Наблюдение запущено. Ожидание новых файлов...")
    logger.info("Для остановки нажмите Ctrl+C")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки...")
        observer.stop()

    observer.join()
    logger.info("Служба остановлена.")


# ─────────────────────────────────────────────
# Windows Service (через pywin32)
# ─────────────────────────────────────────────
try:
    import win32serviceutil
    import win32service
    import win32event
    import servicemanager

    class S3WatcherService(win32serviceutil.ServiceFramework):
        _svc_name_ = "S3FolderWatcher"
        _svc_display_name_ = "S3 Folder Watcher (Timeweb Cloud)"
        _svc_description_ = (
            "Следит за папкой и автоматически загружает новые файлы "
            "в S3-хранилище Timeweb Cloud."
        )

        def __init__(self, args):
            win32serviceutil.ServiceFramework.__init__(self, args)
            self.stop_event = win32event.CreateEvent(None, 0, 0, None)
            self.running = True

        def SvcStop(self):
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            win32event.SetEvent(self.stop_event)
            self.running = False

        def SvcDoRun(self):
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, ""),
            )
            self.main()

        def main(self):
            cfg = load_config()
            logger = setup_logging(cfg.get("log_level", "INFO"))

            logger.info("Windows-служба S3 Folder Watcher запущена")

            try:
                observer, uploader, state = create_watcher(cfg, logger)
            except RuntimeError as e:
                logger.error(str(e))
                return

            observer.start()

            # Периодическое сканирование
            scan_interval = cfg.get("scan_interval_sec", 0)
            if scan_interval > 0:
                scanner = threading.Thread(
                    target=periodic_scan,
                    args=(scan_interval, cfg["watch_folder"], uploader, state, cfg, logger),
                    daemon=True,
                )
                scanner.start()

            logger.info("Наблюдение за папкой запущено (режим службы)")

            # Ожидаем сигнал остановки
            while self.running:
                rc = win32event.WaitForSingleObject(self.stop_event, 1000)
                if rc == win32event.WAIT_OBJECT_0:
                    break

            observer.stop()
            observer.join()
            logger.info("Служба остановлена.")

    HAS_WIN32 = True

except ImportError:
    HAS_WIN32 = False


# ─────────────────────────────────────────────
# Точка входа
# ─────────────────────────────────────────────
def main():
    cfg = load_config()

    # Если передан аргумент для управления службой Windows
    if len(sys.argv) > 1 and sys.argv[1] in (
        "install", "remove", "start", "stop", "restart", "update", "debug"
    ):
        if not HAS_WIN32:
            print("Для работы в режиме Windows-службы установите pywin32:")
            print("  pip install pywin32")
            sys.exit(1)
        win32serviceutil.HandleCommandLine(S3WatcherService)
    else:
        # Обычный режим (консольный)
        run_watcher(cfg)


if __name__ == "__main__":
    main()
