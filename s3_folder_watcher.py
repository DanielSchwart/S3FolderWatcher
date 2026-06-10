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
import fnmatch
import logging
import hashlib
import threading
import queue
from collections import namedtuple
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime, timedelta

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
    "scan_schedule": ["03:00"],
    "scan_interval_sec": 0,
    "upload_existing_on_start": False,
    "file_extensions": [],
    "ignore_patterns": ["*.tmp", "*.partial", "~$*", "Thumbs.db", "desktop.ini"],
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

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Файл с ротацией (5 МБ × 3 архива)
    fh = RotatingFileHandler(
        LOG_PATH, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    fh.setLevel(level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Консоль (в режиме Windows-службы stdout отсутствует)
    if sys.stdout is not None:
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    return logger


# ─────────────────────────────────────────────
# Конфигурация
# ─────────────────────────────────────────────
def load_config(create_if_missing: bool = True) -> dict:
    """Загружает конфигурацию из JSON-файла. Если файла нет — создаёт шаблон."""
    if not CONFIG_PATH.exists():
        if not create_if_missing:
            raise FileNotFoundError(f"Файл конфигурации не найден: {CONFIG_PATH}")
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


def validate_config(cfg: dict) -> list:
    """Возвращает список ошибок конфигурации (пустой список — всё в порядке)."""
    errors = []
    for key in ("s3_access_key", "s3_secret_key", "s3_bucket"):
        value = str(cfg.get(key) or "")
        if not value or value.startswith("ВАШ_") or value == "имя-вашего-бакета":
            errors.append(f"Не заполнен параметр '{key}' в config.json")
    if not str(cfg.get("s3_endpoint", "")).startswith(("http://", "https://")):
        errors.append("Параметр 's3_endpoint' должен начинаться с http:// или https://")
    return errors


# ─────────────────────────────────────────────
# Общая функция фильтрации файлов
# ─────────────────────────────────────────────
def should_ignore(filepath: str, cfg: dict) -> bool:
    """Проверяет, нужно ли игнорировать файл."""
    basename = os.path.basename(filepath)
    for pattern in cfg.get("ignore_patterns", []):
        if any(ch in pattern for ch in "*?["):
            # Glob-паттерн: "*.tmp", "~$*" и т.п.
            if fnmatch.fnmatch(basename, pattern):
                return True
        elif pattern in basename:
            # Простая подстрока (обратная совместимость со старыми конфигами)
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
    """Хранит метаданные (хеш, размер, mtime) загруженных файлов
    для предотвращения повторных загрузок."""

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
        """Проверяет, изменился ли файл. Возвращает (needs_upload, meta)."""
        try:
            st = os.stat(filepath)
        except OSError:
            return False, None

        entry = self.data.get(filepath)
        if isinstance(entry, str):
            # Старый формат состояния: хранился только хеш
            entry = {"hash": entry, "size": None, "mtime": None}

        # Быстрая проверка: размер и mtime не менялись — хеш не пересчитываем
        if entry and entry.get("size") == st.st_size and entry.get("mtime") == st.st_mtime:
            return False, entry

        try:
            current_hash = self.file_hash(filepath)
        except OSError:
            return False, None

        meta = {"hash": current_hash, "size": st.st_size, "mtime": st.st_mtime}
        if entry and entry.get("hash") == current_hash:
            # Содержимое не изменилось — обновляем метаданные без загрузки
            self.mark_uploaded(filepath, meta)
            return False, meta

        return True, meta

    def mark_uploaded(self, filepath: str, meta: dict):
        """Помечает файл как загруженный."""
        with self._lock:
            self.data[filepath] = meta
            self.save()

    def prune_missing(self) -> int:
        """Удаляет записи о файлах, которых больше нет на диске."""
        with self._lock:
            missing = [p for p in self.data if not os.path.exists(p)]
            for p in missing:
                del self.data[p]
            if missing:
                self.save()
        return len(missing)


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

            except EndpointConnectionError:
                self.log.warning(f"  ✗ Нет соединения с {self.cfg['s3_endpoint']}")
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
# Дедупликация одновременных загрузок
# ─────────────────────────────────────────────
class InFlightTracker:
    """Не даёт нескольким потокам одновременно загружать один и тот же файл."""

    def __init__(self):
        self._lock = threading.Lock()
        self._paths: set = set()

    def acquire(self, path: str) -> bool:
        with self._lock:
            if path in self._paths:
                return False
            self._paths.add(path)
            return True

    def release(self, path: str):
        with self._lock:
            self._paths.discard(path)


def try_upload_path(path: str, uploader: S3Uploader, state: UploadState,
                    cfg: dict, in_flight: InFlightTracker) -> bool:
    """Единая точка загрузки файла: дедупликация, проверка изменений, загрузка."""
    if not in_flight.acquire(path):
        return False
    try:
        needs, meta = state.needs_upload(path)
        if not needs:
            return False
        if uploader.upload_file(path, cfg["watch_folder"]):
            state.mark_uploaded(path, meta)
            return True
        return False
    finally:
        in_flight.release(path)


# ─────────────────────────────────────────────
# Обработчик событий файловой системы
# ─────────────────────────────────────────────
class FolderEventHandler(FileSystemEventHandler):
    def __init__(self, uploader: S3Uploader, state: UploadState,
                 cfg: dict, logger: logging.Logger, in_flight: InFlightTracker):
        super().__init__()
        self.uploader = uploader
        self.state = state
        self.cfg = cfg
        self.log = logger
        self.in_flight = in_flight
        self._stop = threading.Event()
        self._debounce_sec = 2.0
        # Очередь + один рабочий поток вместо множества Timer-ов
        self._pending: dict[str, float] = {}
        self._pending_lock = threading.Lock()
        self._queue: queue.Queue = queue.Queue()
        self._worker = threading.Thread(target=self._upload_worker, daemon=True)
        self._worker.start()

    def stop(self, timeout: float = 30.0):
        """Останавливает рабочий поток, дождавшись завершения текущей загрузки."""
        self._stop.set()
        self._worker.join(timeout=timeout)

    def _upload_worker(self):
        """Единый рабочий поток для обработки файлов с debounce."""
        while not self._stop.is_set():
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
                self.log.debug(f"Файл всё ещё занят: {path}, будет загружен при сканировании")
                return

        try_upload_path(path, self.uploader, self.state, self.cfg, self.in_flight)

    @staticmethod
    def _is_file_ready(path: str) -> bool:
        """Проверяет, что файл доступен и не занят другим процессом."""
        try:
            with open(path, "rb"):
                pass
        except (IOError, PermissionError):
            return False
        if os.name == "nt":
            # На Windows переименование файла в самого себя не удаётся,
            # пока он открыт другим процессом (например, идёт копирование)
            try:
                os.replace(path, path)
            except OSError:
                return False
        return True

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
_scan_lock = threading.Lock()


def scan_existing_files(watch_folder: str, uploader: S3Uploader,
                        state: UploadState, cfg: dict, logger: logging.Logger,
                        in_flight: InFlightTracker):
    """Сканирует папку и загружает файлы, которые ещё не были загружены."""
    if not _scan_lock.acquire(blocking=False):
        logger.info("Сканирование уже выполняется — пропуск.")
        return
    try:
        logger.info(f"Сканирование существующих файлов в: {watch_folder}")
        count = 0

        for root, dirs, files in os.walk(watch_folder):
            for filename in files:
                filepath = os.path.join(root, filename)

                if should_ignore(filepath, cfg):
                    continue

                if try_upload_path(filepath, uploader, state, cfg, in_flight):
                    count += 1

        pruned = state.prune_missing()
        if pruned:
            logger.info(f"Удалено записей об отсутствующих файлах: {pruned}")

        logger.info(f"Сканирование завершено. Загружено файлов: {count}")
    finally:
        _scan_lock.release()


# ─────────────────────────────────────────────
# Общая логика инициализации watcher
# ─────────────────────────────────────────────
WatcherParts = namedtuple(
    "WatcherParts", ["observer", "handler", "uploader", "state", "in_flight"]
)


def create_watcher(cfg: dict, logger: logging.Logger) -> WatcherParts:
    """Создаёт и возвращает настроенный Observer и сопутствующие объекты."""
    watch_folder = cfg["watch_folder"]
    if not os.path.isdir(watch_folder):
        logger.info(f"Создание папки для наблюдения: {watch_folder}")
        os.makedirs(watch_folder, exist_ok=True)

    uploader = S3Uploader(cfg, logger)
    if not uploader.test_connection():
        raise RuntimeError("Не удалось подключиться к S3. Проверьте config.json")

    state = UploadState(STATE_PATH)
    in_flight = InFlightTracker()

    if cfg.get("upload_existing_on_start", False):
        scan_existing_files(watch_folder, uploader, state, cfg, logger, in_flight)

    handler = FolderEventHandler(uploader, state, cfg, logger, in_flight)
    observer = Observer()
    observer.schedule(handler, watch_folder, recursive=True)

    return WatcherParts(observer, handler, uploader, state, in_flight)


# ─────────────────────────────────────────────
# Интервальное сканирование (подстраховка)
# ─────────────────────────────────────────────
def periodic_scan(interval: int, watch_folder: str, uploader: S3Uploader,
                  state: UploadState, cfg: dict, logger: logging.Logger,
                  in_flight: InFlightTracker):
    """Периодически сканирует папку для подстраховки (watchdog может пропустить события)."""
    while True:
        time.sleep(interval)
        try:
            scan_existing_files(watch_folder, uploader, state, cfg, logger, in_flight)
        except Exception as e:
            logger.warning(f"Ошибка при периодическом сканировании: {e}")


# ─────────────────────────────────────────────
# Сканирование по расписанию
# ─────────────────────────────────────────────
def parse_schedule(schedule_list: list, logger: logging.Logger) -> list:
    """Парсит список времён из конфига. Формат: ['03:00', '14:30']."""
    parsed = []
    for item in schedule_list:
        try:
            parts = item.strip().split(":")
            hour = int(parts[0])
            minute = int(parts[1]) if len(parts) > 1 else 0
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                parsed.append((hour, minute))
            else:
                logger.warning(f"Некорректное время в расписании: {item}")
        except (ValueError, IndexError):
            logger.warning(f"Не удалось разобрать время: {item}")
    return sorted(parsed)


def next_run_time(schedule: list, after: datetime) -> datetime:
    """Возвращает ближайший момент запуска из расписания после момента `after`."""
    candidates = []
    for hour, minute in schedule:
        candidate = after.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= after:
            candidate += timedelta(days=1)
        candidates.append(candidate)
    return min(candidates)


def scheduled_scan(schedule: list, watch_folder: str, uploader: S3Uploader,
                   state: UploadState, cfg: dict, logger: logging.Logger,
                   in_flight: InFlightTracker):
    """Запускает сканирование в заданное время. Если момент был пропущен
    (сон системы, долгое предыдущее сканирование) — выполняет его при
    первой возможности."""
    next_run = next_run_time(schedule, datetime.now())
    logger.info(f"Следующее сканирование по расписанию: {next_run:%Y-%m-%d %H:%M}")

    while True:
        time.sleep(20)
        if datetime.now() < next_run:
            continue

        logger.info(f"Запланированное сканирование ({next_run:%H:%M})...")
        try:
            scan_existing_files(watch_folder, uploader, state, cfg, logger, in_flight)
        except Exception as e:
            logger.warning(f"Ошибка при сканировании по расписанию: {e}")

        next_run = next_run_time(schedule, datetime.now())
        logger.info(f"Следующее сканирование по расписанию: {next_run:%Y-%m-%d %H:%M}")


# ─────────────────────────────────────────────
# Запуск фоновых сканирований (общий для консоли и службы)
# ─────────────────────────────────────────────
def start_background_scans(cfg: dict, uploader: S3Uploader, state: UploadState,
                           logger: logging.Logger, in_flight: InFlightTracker):
    """Запускает потоки интервального и планового сканирования (если включены)."""
    scan_interval = cfg.get("scan_interval_sec", 0)
    if scan_interval > 0:
        threading.Thread(
            target=periodic_scan,
            args=(scan_interval, cfg["watch_folder"], uploader, state, cfg,
                  logger, in_flight),
            daemon=True,
        ).start()
        logger.info(f"Интервальное сканирование каждые {scan_interval} сек.")

    schedule_list = cfg.get("scan_schedule", [])
    if schedule_list:
        schedule = parse_schedule(schedule_list, logger)
        if schedule:
            threading.Thread(
                target=scheduled_scan,
                args=(schedule, cfg["watch_folder"], uploader, state, cfg,
                      logger, in_flight),
                daemon=True,
            ).start()
            times_str = ", ".join(f"{h:02d}:{m:02d}" for h, m in schedule)
            logger.info(f"Сканирование по расписанию: {times_str}")


# ─────────────────────────────────────────────
# Основной цикл работы (не-сервисный режим)
# ─────────────────────────────────────────────
def run_watcher(cfg: dict):
    """Запускает наблюдатель за папкой."""
    logger = setup_logging(cfg.get("log_level", "INFO"))

    errors = validate_config(cfg)
    if errors:
        for err in errors:
            logger.error(err)
        logger.error("Исправьте config.json и запустите программу снова.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("  S3 Folder Watcher — Timeweb Cloud")
    logger.info(f"  Endpoint:  {cfg['s3_endpoint']}")
    logger.info(f"  Bucket:    {cfg['s3_bucket']}")
    logger.info(f"  Папка:     {cfg['watch_folder']}")
    logger.info("=" * 60)

    parts = create_watcher(cfg, logger)
    parts.observer.start()

    start_background_scans(cfg, parts.uploader, parts.state, logger, parts.in_flight)

    logger.info("Наблюдение запущено. Ожидание новых файлов...")
    logger.info("Для остановки нажмите Ctrl+C")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки...")

    parts.observer.stop()
    parts.observer.join()
    parts.handler.stop()
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
            try:
                cfg = load_config(create_if_missing=False)
            except FileNotFoundError as e:
                servicemanager.LogErrorMsg(str(e))
                return

            logger = setup_logging(cfg.get("log_level", "INFO"))

            errors = validate_config(cfg)
            if errors:
                for err in errors:
                    logger.error(err)
                servicemanager.LogErrorMsg(
                    "Некорректная конфигурация. Подробности в " + str(LOG_PATH)
                )
                return

            logger.info("Windows-служба S3 Folder Watcher запущена")

            try:
                parts = create_watcher(cfg, logger)
            except RuntimeError as e:
                logger.error(str(e))
                return

            parts.observer.start()
            start_background_scans(cfg, parts.uploader, parts.state,
                                   logger, parts.in_flight)

            logger.info("Наблюдение за папкой запущено (режим службы)")

            # Ожидаем сигнал остановки
            while self.running:
                rc = win32event.WaitForSingleObject(self.stop_event, 1000)
                if rc == win32event.WAIT_OBJECT_0:
                    break

            parts.observer.stop()
            parts.observer.join()
            parts.handler.stop()
            logger.info("Служба остановлена.")

    HAS_WIN32 = True

except ImportError:
    HAS_WIN32 = False


# ─────────────────────────────────────────────
# Автоустановка службы
# ─────────────────────────────────────────────
def is_admin() -> bool:
    """Проверяет, запущен ли процесс с правами администратора."""
    try:
        import ctypes
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except Exception:
        return False


def run_as_admin():
    """Перезапускает текущий процесс с правами администратора."""
    import ctypes
    if getattr(sys, 'frozen', False):
        # EXE запускает сам себя — argv[0] (путь к exe) в параметрах не нужен
        args = sys.argv[1:]
    else:
        # python.exe + путь к скрипту + аргументы
        args = sys.argv
    params = " ".join(f'"{a}"' for a in args)
    ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, params, None, 1)


def get_service_command() -> str:
    """Команда запуска службы для SCM (binPath)."""
    if getattr(sys, 'frozen', False):
        return f'"{sys.executable}" --service'
    script = os.path.abspath(sys.argv[0])
    return f'"{sys.executable}" "{script}" --service'


def is_service_installed(service_name: str) -> bool:
    """Проверяет, установлена ли служба."""
    try:
        import subprocess
        result = subprocess.run(
            ["sc", "query", service_name],
            capture_output=True, text=True
        )
        return result.returncode == 0
    except Exception:
        return False


def is_service_running(service_name: str) -> bool:
    """Проверяет, запущена ли служба."""
    try:
        import subprocess
        result = subprocess.run(
            ["sc", "query", service_name],
            capture_output=True, text=True
        )
        return "RUNNING" in result.stdout
    except Exception:
        return False


def auto_install_and_start():
    """Автоматически устанавливает и запускает службу Windows."""
    service_name = "S3FolderWatcher"

    # Конфигурацию проверяем ДО установки службы: если файла нет,
    # load_config создаст шаблон и завершит работу с подсказкой
    cfg = load_config()
    errors = validate_config(cfg)
    if errors:
        for err in errors:
            print(f"[!] {err}")
        print("Отредактируйте config.json и запустите программу снова.")
        input("Нажмите Enter для выхода...")
        sys.exit(1)

    if not is_admin():
        print("Для установки службы требуются права администратора.")
        print("Запрашиваю повышение прав...")
        run_as_admin()
        sys.exit(0)

    import subprocess

    if is_service_installed(service_name):
        print(f"Служба '{service_name}' уже установлена.")
        # Обновляем на случай, если exe переместили
        subprocess.run(
            ["sc", "config", service_name, "binPath=", get_service_command()],
            capture_output=True
        )
        # Устанавливаем автозапуск
        subprocess.run(
            ["sc", "config", service_name, "start=", "auto"],
            capture_output=True
        )
        if not is_service_running(service_name):
            print("Запускаю службу...")
            subprocess.run(["sc", "start", service_name], capture_output=True)
            time.sleep(2)
            if is_service_running(service_name):
                print(f"✓ Служба '{service_name}' запущена!")
            else:
                print(f"✗ Не удалось запустить службу. Проверьте журнал: {LOG_PATH}")
        else:
            print(f"✓ Служба '{service_name}' уже работает.")
    else:
        print(f"Установка службы '{service_name}'...")

        # Регистрируем службу через sc create
        result = subprocess.run(
            ["sc", "create", service_name,
             "binPath=", get_service_command(),
             "DisplayName=", "S3 Folder Watcher (Timeweb Cloud)",
             "start=", "auto"],
            capture_output=True, text=True
        )

        if result.returncode != 0:
            print(f"✗ Ошибка установки: {result.stderr.strip()}")
            input("Нажмите Enter для выхода...")
            sys.exit(1)

        # Устанавливаем описание
        subprocess.run(
            ["sc", "description", service_name,
             "Следит за папкой и автоматически загружает файлы в S3-хранилище Timeweb Cloud."],
            capture_output=True
        )

        # Настраиваем автоперезапуск при сбое (через 10 секунд)
        subprocess.run(
            ["sc", "failure", service_name,
             "reset=", "86400", "actions=", "restart/10000/restart/10000/restart/30000"],
            capture_output=True
        )

        print(f"✓ Служба '{service_name}' установлена (автозапуск).")
        print("Запускаю службу...")

        subprocess.run(["sc", "start", service_name], capture_output=True)
        time.sleep(2)

        if is_service_running(service_name):
            print(f"✓ Служба '{service_name}' успешно запущена!")
        else:
            print(f"✗ Не удалось запустить. Проверьте журнал: {LOG_PATH}")

    print()
    print(f"  Папка наблюдения: {cfg['watch_folder']}")
    print(f"  Лог-файл:        {LOG_PATH}")
    print(f"  Конфигурация:     {CONFIG_PATH}")
    print()
    print("  Управление:")
    print(f'    sc stop {service_name}     — остановить')
    print(f'    sc start {service_name}    — запустить')
    print(f'    sc delete {service_name}   — удалить')
    print()
    input("Нажмите Enter для выхода...")


# ─────────────────────────────────────────────
# Точка входа
# ─────────────────────────────────────────────
def main():
    # Режим службы Windows (вызывается SCM)
    if "--service" in sys.argv:
        if not HAS_WIN32:
            print("pywin32 не найден!")
            sys.exit(1)
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(S3WatcherService)
        servicemanager.StartServiceCtrlDispatcher()
        return

    # Ручное управление службой (install/remove/start/stop)
    if len(sys.argv) > 1 and sys.argv[1] in (
        "install", "remove", "start", "stop", "restart", "update", "debug"
    ):
        if not HAS_WIN32:
            print("Для работы в режиме Windows-службы установите pywin32:")
            print("  pip install pywin32")
            sys.exit(1)
        win32serviceutil.HandleCommandLine(S3WatcherService)
        return

    # Режим консоли (--console)
    if "--console" in sys.argv:
        cfg = load_config()
        run_watcher(cfg)
        return

    # По умолчанию: автоустановка и запуск службы
    auto_install_and_start()


if __name__ == "__main__":
    main()
