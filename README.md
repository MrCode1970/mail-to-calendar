# mail-to-calendar

Автоматизация для Gmail и Google Calendar: скрипт ждёт письмо в Gmail и создаёт напоминания в Google Calendar.

## Типы событий

- `ALLDAY` — визуальный якорь на день (весь день), помогает видеть факт ожидания.
- `TAIL` — боевое событие с напоминаниями, чтобы не пропустить действие.

## Режимы работы

- `TEST` — использует последнее реальное письмо, но `receivedAt = now` (для проверки сценария на свежем времени).
- `LIVE` — обрабатывает новые письма и помечает треды ярлыком.

## Quiet hours

Есть тихие часы: 23:00–07:00 (Asia/Jerusalem). В этот период логика учитывает ограничение по времени уведомлений.

## Вариант B (рекомендуется): GitHub + clasp + Apps Script

Это правильный поток, если хотите, чтобы кодовая версия и боевой скрипт всегда совпадали.

1. Разработка локально (VS Code/Codex), изменения в `Code.js` и `appsscript.json`.
2. Проверка и коммит:
   - `node --check Code.js`
   - `git add . && git commit -m "..."`
3. Публикация в GitHub:
   - `git push origin <your-branch>`
   - открыть PR, обсудить, сделать Merge.
4. Синхронизация в Apps Script через `clasp push`.
5. Тестирование в Apps Script IDE (например, `runTestOnce`) и проверка триггеров.

Важно: PR на GitHub сам по себе не обновляет Apps Script. Обновление боевого скрипта выполняется только через `clasp push` (или ручным копированием).

### Быстрый старт clasp (один раз)

```bash
npm i -g @google/clasp
clasp login
clasp clone <SCRIPT_ID>
```

Если проект уже локально в git, вместо clone:

```bash
clasp create --type standalone --title "Mail-to-Calendar"
# или привязать существующий проект
clasp setting scriptId <SCRIPT_ID>
```

### Ежедневный цикл (коротко)

```bash
# 1) изменили код
node --check Code.js

# 2) зафиксировали версию
git add .
git commit -m "feat: ..."
git push origin <branch>

# 3) после merge в main
git checkout main
git pull
clasp push
```

## Базовый рабочий процесс

- Разработка в VS Code.
- `clasp push` для синхронизации с Apps Script.
- Запуск через Apps Script IDE или по триггеру.
