# Systemd Deploy

Используй `systemd`, чтобы бот всегда работал в одном экземпляре и автоматически перезапускался после падения или ребута сервера.

## Установка

Скопируй unit-файл на сервер:

```bash
cd /root/beer_dog_bot
mkdir -p /etc/systemd/system
cp deploy/systemd/beer-dog-bot.service /etc/systemd/system/beer-dog-bot.service
```

Останови старые ручные процессы:

```bash
pkill -9 -f "bot.py" || true
```

Перечитай unit-файлы и включи сервис:

```bash
systemctl daemon-reload
systemctl enable beer-dog-bot
systemctl restart beer-dog-bot
```

## Проверка

Статус:

```bash
systemctl status beer-dog-bot --no-pager
```

Логи:

```bash
journalctl -u beer-dog-bot -n 100 --no-pager
```

Живой лог:

```bash
journalctl -u beer-dog-bot -f
```

## Обновление после git pull

```bash
cd /root/beer_dog_bot
git fetch origin
git checkout main
git pull --ff-only origin main
source venv/bin/activate
python -m pip install -r requirements.txt
systemctl restart beer-dog-bot
systemctl status beer-dog-bot --no-pager
```

## Полезные команды

Остановить:

```bash
systemctl stop beer-dog-bot
```

Запустить:

```bash
systemctl start beer-dog-bot
```

Перезапустить:

```bash
systemctl restart beer-dog-bot
```
