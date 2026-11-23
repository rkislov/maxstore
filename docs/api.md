# HTTP API

Консоль и API используют `HTTP_ADDR`, по умолчанию порт 8080. Все ответы API — JSON, кроме содержимого объекта и ответов 204. `GET /healthz` доступен без авторизации и подтверждает работу HTTP-сервера; доступность Telegram/MAX не проверяет.

## Вход

`POST /api/auth/login` с `Content-Type: application/json` и телом `{"username":"admin","password":"..."}` выдаёт HttpOnly SameSite=Strict cookie. Сессия живёт 8 часов. `GET /api/auth/session` возвращает текущую сессию; `POST /api/auth/logout` завершает её. Для изменяющих запросов из браузера обязателен `X-Maxstore-Request: 1`.

Для скриптов поддержан `Authorization: Bearer <console_token>` из credentials.json. Это административный токен всего HTTP API. Не используйте S3-ключи как bearer.

## Бакеты и объекты

- `GET /api/status`: бакеты, провайдеры, объём, число объектов, лимиты, активные операции и endpoint S3.
- `POST /api/buckets`: `{"name":"my-backups","provider":"telegram"}`; ответ 201.
- `DELETE /api/buckets/{name}`: удаление пустого бакета, ответ 204.
- `GET /api/buckets/{name}?prefix=folder/`: список объектов.
- `PUT /api/objects?bucket=...&key=...`: тело объекта, Content-Type; ответ 201.
- `GET` / `HEAD /api/objects?bucket=...&key=...`: поток или заголовки; GET поддерживает Range.
- `DELETE /api/objects?bucket=...&key=...`: удаление, ответ 204.

Все параметры query кодируются URLSearchParams/urlencode, включая ключи с Unicode, пробелами и слешами.

## Загрузка частями

1. `POST /api/uploads` с `{"bucket":"my-backups","key":"large.bin","type":"application/octet-stream","size":123456789,"parallel":true}`. Ответ содержит `id`, `mode`, `size`.
2. `PUT /api/uploads/{id}/parts/{number}`: номер от 1, бинарное тело до 8 МиБ. Все части, кроме последней, должны быть ровно 8 МиБ. Заголовок `X-Content-SHA256` содержит hex SHA-256 части. Части можно передавать не по порядку и повторять идемпотентно с тем же содержимым.
3. `GET /api/uploads/{id}` и `GET /api/uploads/{id}/parts` восстанавливают состояние.
4. `POST /api/uploads/{id}/complete` публикует объект только при наличии всех частей. Для пустого файла части не нужны.
5. `DELETE /api/uploads/{id}` прерывает сессию и ставит известные части в очередь очистки.

`parallel:false` сохраняет совместимость с прежним последовательным протоколом. Новым клиентам следует явно передавать `parallel:true`. UI upload-сессии и S3 multipart разделены по режиму.

Ошибки: 400 некорректные параметры/часть/сумма, 401 нет сессии, 403 CSRF, 404 нет ресурса, 409 конфликт/непустой бакет, 412 условие записи, 413 лимит, 502 ошибка хранилища/провайдера. Тело содержит `error`.
