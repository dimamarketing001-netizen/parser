# ParsYa Control Plane

Веб-кабинет для удалённого управления парсером ParsYa, который продолжает работать в WSL. Сервер кабинета **не подключается к WSL входящими соединениями**: WSL-worker сам устанавливает HTTPS-соединение и забирает задания из очереди.

## Архитектура

- `app/main.py` — FastAPI, авторизация администратора, очередь заданий, статус воркеров, история запусков и библиотека креативов.
- `worker/agent.py` — агент для WSL. Получает только типизированную задачу `run_parser`, запускает заранее заданный файл парсера, передаёт лог, после завершения синхронизирует результаты и HTML из локального MySQL.
- `worker/patch_parser_env.py` — аккуратно добавляет поддержку `PARSYA_CITY` и `PARSYA_KEY_TEXT` в существующий скрипт; перед изменением создаёт резервную копию.
- SQLite на веб-сервере хранит очередь, историю, метаданные и сжатые HTML-креативы. MySQL в WSL остаётся основной локальной БД ParsYa.

Панель не принимает и не выполняет произвольные shell-команды. Открывать порты или настраивать проброс портов к WSL не требуется.

## 1. Установка веб-панели на сервер

Инструкция предполагает Ubuntu/Debian, существующий Nginx и свободный локальный порт 8091.

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip nginx certbot python3-certbot-nginx

sudo useradd --system --home-dir /var/lib/parsya-control --shell /usr/sbin/nologin parsya-control 2>/dev/null || true
sudo install -d -o parsya-control -g parsya-control /var/lib/parsya-control

sudo git clone --branch main https://github.com/dimamarketing001-netizen/parser.git /opt/parsya-control
sudo python3 -m venv /opt/parsya-control/.venv
sudo /opt/parsya-control/.venv/bin/pip install --upgrade pip
sudo /opt/parsya-control/.venv/bin/pip install -r /opt/parsya-control/requirements.txt
sudo chown -R root:root /opt/parsya-control
sudo chown -R parsya-control:parsya-control /opt/parsya-control/.venv
```

Сгенерируй три отдельных секрета:

```bash
openssl rand -hex 32   # повторить отдельно для каждого секрета
```

Создай файл окружения вне репозитория:

```bash
sudo cp /opt/parsya-control/.env.example /etc/parsya-control.env
sudo nano /etc/parsya-control.env
sudo chmod 600 /etc/parsya-control.env
sudo chown root:root /etc/parsya-control.env
```

Установи **разные** случайные значения для `PARSYA_ADMIN_PASSWORD`, `PARSYA_SECRET_KEY` и `PARSYA_WORKER_TOKEN`. Значение `PARSYA_WORKER_TOKEN` позже нужно повторить в WSL. Не коммить секреты в Git. Для `PARSYA_COOKIE_SECURE` используй `1` после настройки HTTPS.

Подключи системный сервис:

```bash
sudo cp /opt/parsya-control/deploy/parsya-control.service /etc/systemd/system/parsya-control.service
sudo systemctl daemon-reload
sudo systemctl enable --now parsya-control
sudo systemctl status parsya-control --no-pager
curl -fsS http://127.0.0.1:8091/health
```

Ожидаемый health-ответ: `{"status":"ok","service":"parsya-control-plane"}`.

## 2. Nginx и HTTPS для parser.союзюристовроссии.рф

IDN-домен в ASCII (punycode): `xn--80ak2afbe.xn--b1ajdba5acbodeeeaj1qb.xn--p1ai`.

Сначала в DNS создай/проверь A-запись поддомена, указывающую на публичный IPv4 сервера. Не меняй существующие конфиги главного домена и других поддоменов.

```bash
sudo cp /opt/parsya-control/deploy/nginx-parser.conf /etc/nginx/sites-available/parser-souz.conf
sudo ln -s /etc/nginx/sites-available/parser-souz.conf /etc/nginx/sites-enabled/parser-souz.conf
sudo nginx -t
sudo systemctl reload nginx
```

Если ссылка уже существует, не создавай повторный symlink. После того как DNS указывает на сервер и порт 80 доступен снаружи, получи сертификат:

```bash
sudo certbot --nginx -d xn--80ak2afbe.xn--b1ajdba5acbodeeeaj1qb.xn--p1ai
```

Заверши настройку, убедись, что `PARSYA_COOKIE_SECURE=1` в `/etc/parsya-control.env`, затем:

```bash
sudo systemctl restart parsya-control
```

Открывай `https://парсер.союзюристовроссии.рф` и войди с администраторским логином/паролем из файла окружения.

## 3. Подготовить WSL

В Windows/WSL не требуется входящий порт, статический публичный IP или проброс портов. Worker обращается к панели через исходящий HTTPS.

Клонируй worker-код отдельно от основного проекта, чтобы не перезаписать работающие файлы ParsYa:

```bash
cd /home/dima/ParsYa
git clone --branch main https://github.com/dimamarketing001-netizen/parser.git parsya-control
source .venv/bin/activate
pip install -r /home/dima/ParsYa/parsya-control/worker/requirements.txt
```

Патчер меняет только присваивания города и запроса и создаёт резервную копию. Укажи **фактический** путь к тому скрипту, который сейчас используешь:

```bash
python /home/dima/ParsYa/parsya-control/worker/patch_parser_env.py /home/dima/ParsYa/ParsYa_main_with_tracking.py
```

Если основной файл называется иначе, поменяй путь. Не запускай два экземпляра Selenium-парсера одновременно: они используют общий Chrome/CDP-порт 9222.

Добавь в существующий `/home/dima/ParsYa/.env` следующие строки. Не заменяй и не удаляй существующие параметры MySQL / прокси:

```dotenv
PARSYA_CONTROL_URL=https://парсер.союзюристовроссии.рф
PARSYA_WORKER_TOKEN=ТОЧНО_ТОТ_ЖЕ_СЕКРЕТ_ЧТО_НА_ВЕБ-СЕРВЕРЕ
PARSYA_WORKER_ID=wsl-ekaterinburg-01
PARSYA_ENV_FILE=/home/dima/ParsYa/.env
PARSYA_HOME=/home/dima/ParsYa
PARSYA_MAIN_SCRIPT=/home/dima/ParsYa/ParsYa_main_with_tracking.py
PARSYA_PYTHON=/home/dima/ParsYa/.venv/bin/python
PARSYA_SYNC_MYSQL=1
PARSYA_POLL_SECONDS=5
```

Защити файл:

```bash
chmod 600 /home/dima/ParsYa/.env
```

Проверь, что в нём по-прежнему есть `PARSYA_DB_PASSWORD`, `PARSYA_DB_HOST`, `PARSYA_DB_USER` и `PARSYA_DB_NAME`, поскольку воркер после запуска читает результаты/HTML из локального MySQL и отправляет их в кабинет.

## 4. Проверить связь перед автозапуском

Сначала оставь worker в foreground, чтобы увидеть ошибки:

```bash
source /home/dima/ParsYa/.venv/bin/activate
python /home/dima/ParsYa/parsya-control/worker/agent.py
```

В журнале должно появиться сообщение о подключении к control plane. Открой кабинет, создай запуск и проверь, что задание переходит из `queued` в `running`. Для первого теста используй заведомо рабочие параметры и не запускай тот же парсер вручную одновременно.

Остановить агент в foreground: `Ctrl+C`.

## 5. Автозапуск воркера через systemd в WSL

Проверь поддержку systemd:

```bash
ps -p 1 -o comm=
```

Если вернулось `systemd`, установи сервис:

```bash
sudo cp /home/dima/ParsYa/parsya-control/deploy/parsya-wsl-worker.service /etc/systemd/system/parsya-wsl-worker.service
sudo systemctl daemon-reload
sudo systemctl enable --now parsya-wsl-worker
sudo systemctl status parsya-wsl-worker --no-pager
journalctl -u parsya-wsl-worker -f
```

Если PID 1 не systemd, включи его в WSL через `/etc/wsl.conf`:

```ini
[boot]
systemd=true
```

Затем из Windows выполни `wsl --shutdown`, заново открой дистрибутив Ubuntu и повтори проверку. Это перезапустит WSL-среду, поэтому сначала заверши текущие задания.

## 6. Как будет ходить задание

1. Ты входишь в кабинет и создаёшь запуск (город + запрос).
2. Сервер сохраняет его в очереди SQLite.
3. WSL-worker раз в 5 секунд делает HTTPS poll; при наличии задания сервер возвращает только типизированные параметры.
4. Worker выставляет `PARSYA_CITY` и `PARSYA_KEY_TEXT`, запускает выбранный локальный скрипт и передаёт лог/статус.
5. После успешного завершения worker читает результаты последнего соответствующего запуска из локального MySQL, отправляет метаданные и HTML в веб-панель.
6. Креативы и история обнаружений становятся доступны в библиотеке.

## Ограничения первой версии

- SQLite и один воркер — стартовая конфигурация; это не кластер и не система параллельного запуска браузеров.
- Синхронизация HTML ограничивает размер одного HTML-фрагмента 2 МБ; метаданные остаются доступны, даже если слишком большой HTML пропущен.
- Синхронизация выбирает последний MySQL-run по городу и точному ключевому запросу. Пока worker работает, не запускай одновременно тот же основной скрипт вручную.
- Web-интерфейс пока рассчитан на одного администратора. Для нескольких пользователей нужен отдельный слой ролей.
- Никогда не открывай worker API без HTTPS и не передавай токен в URL. Если токен попал в логи/репозиторий — немедленно поменяй его с обеих сторон.

## Диагностика

```bash
# Веб-сервер
sudo journalctl -u parsya-control -n 200 --no-pager
curl -i http://127.0.0.1:8091/health

# WSL worker
sudo journalctl -u parsya-wsl-worker -n 200 --no-pager

# Публичная сторона/Nginx
sudo nginx -t
sudo tail -n 100 /var/log/nginx/error.log
```
