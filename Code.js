/*************************************************************
 * PROJECT: Mail → Calendar V3
 *
 * Цель:
 *  - ждать письмо от queue-mailer@kdmid.ru (действительно 24 часа)
 *  - создать в Google Calendar события, чтобы сложно было пропустить уведомления
 *
 * Модель: 4 вида событий
 *  1) ALLDAY (сегодня) — визуальный якорь “как ДР”
 *  2) LONG (сутки) — старт now+10мин, конец receivedAt+24ч,
 *     напоминания за 8/6/4/2 минут и в момент старта
 *  3) HOURLY-CHAIN — часовые сигналы вне тихого времени
 *     (создаётся/проверяется цепочкой, прекращается при удалении)
 *  4) FINAL — финальное событие перед дедлайном
 *
 * Режимы:
 *  - LIVE: берём новые письма from:sender -label:processed newer_than:2d
 *  - TEST: берём ПОСЛЕДНЕЕ реальное письмо от sender (subject/thread/link/realDate),
 *          но считаем, что оно “пришло сейчас” (receivedAt=now) — для теста “пищит ли”.
 *
 * Логи:
 *  - Executions → Logs (console.log) brief/verbose
 *  - Google Sheets лог: MailToCalendarLog / лист Log
 *************************************************************/

/***********************
 * CONFIG
 ***********************/
const CONFIG = {
  // Ожидаемый отправитель
  EXPECTED_SENDER_EMAIL: "queue-mailer@kdmid.ru",

  // Твой ящик (для описания/логов)
  YOUR_INBOX_EMAIL: "misterx1970@gmail.com",

  // Для LIVE: помечаем обработанные треды ярлыком
  PROCESSED_LABEL_NAME: "MailAlertProcessed",

  // Префикс заголовков событий
  EVENT_PREFIX: "MAIL ALERT",

  // “Письмо действительно” (часов)
  ACTIVE_WINDOW_HOURS: 24,

  // LONG: старт now + 10 минут
  EVENT1_START_PLUS_MINUTES: 10,

  // LONG: напоминания в минутах до старта
  EVENT1_REMINDERS_MINUTES: [8, 6, 4, 2, 0],

  // HOURLY-CHAIN: шаг сигналов (часы)
  HOURLY_INTERVAL_HOURS: 1,

  // HOURLY-CHAIN: максимум сигналов в одном событии
  HOURLY_BLOCK_MAX_SIGNALS: 5,

  // FINAL: напоминания в минутах до старта
  FINAL_REMINDERS_MINUTES: [40, 30, 20, 10, 0],

  // Тихий интервал (по Израилю — timezone скрипта/аккаунта)
  QUIET_HOUR_START: 23, // 23:00
  QUIET_HOUR_END: 7,    // 07:00
  QUIET_SET_TO_HOUR: 7, // старт tail ставим на 07:00

  // Sheets лог
  LOG_SPREADSHEET_NAME: "MailToCalendarLog",
  LOG_SHEET_NAME: "Log",

  // Встроенный лог
  SCRIPT_LOG_LEVEL: "verbose", // "brief" | "verbose"

  // Режим работы
  TEST_MODE: false,

  // В тесте: искать последнее письмо за N дней
  TEST_LOOKBACK_DAYS: 60,

  // В LIVE: период поиска новых писем
  LIVE_NEWER_THAN_DAYS: 2,
};

/***********************
 * MAIN ENTRYPOINT
 ***********************/
function checkMailAndCreateTwoEvents() {
  const runId = newRunId_();
  const lock = LockService.getScriptLock();
  const lockAcquired = lock.tryLock(5000);

  if (!lockAcquired) {
    slogErr_(runId, "LOCK_BUSY", "Пропуск запуска: предыдущий запуск ещё выполняется", {});
    sheetLog_(runId, CONFIG.TEST_MODE ? "TEST" : "LIVE", "LOCK_BUSY", "Пропуск: активен предыдущий запуск", {});
    return;
  }

  slogInfo_(runId, "START", "Запуск", {
    testMode: CONFIG.TEST_MODE,
    expectedSender: CONFIG.EXPECTED_SENDER_EMAIL,
    inbox: CONFIG.YOUR_INBOX_EMAIL,
    tz: Session.getScriptTimeZone(),
    logLevel: CONFIG.SCRIPT_LOG_LEVEL,
  });

  try {
    if (CONFIG.TEST_MODE) {
      // TEST: берём последнее реальное письмо для контекста, но receivedAt = NOW
      const latest = findLatestMailFromSender_(runId, CONFIG.EXPECTED_SENDER_EMAIL, CONFIG.TEST_LOOKBACK_DAYS);
      const now = new Date();

      const mail = {
        mode: "TEST",
        receivedAt: now, // ВАЖНО: тест всегда "как будто сейчас"
        subject: latest ? latest.subject : "TEST: не найдено писем — симуляция",
        threadId: latest ? latest.threadId : "TEST_THREAD_NOW",
        gmailLink: latest ? latest.gmailLink : "https://mail.google.com/mail/u/0/#inbox",
        meta: {
          test_used_latest_real_mail: Boolean(latest),
          realMailReceivedAt: latest ? latest.realReceivedAt.toString() : "",
          realMailMessageId: latest ? latest.messageId : "",
          lookbackDays: CONFIG.TEST_LOOKBACK_DAYS,
          note: "TEST: контекст реального письма, но receivedAt=NOW"
        }
      };

      slogInfo_(runId, "TEST_MODE", "TEST: используем контекст последнего письма, но receivedAt=NOW", {
        now: now.toString(),
        usedLatest: Boolean(latest),
        subject: mail.subject,
        threadId: mail.threadId,
        realMailReceivedAt: mail.meta.realMailReceivedAt
      });

      createTwoEventsForMail_(runId, mail);
      slogOk_(runId, "DONE_TEST", "Тест завершён", {});
      return;
    }

    // LIVE
    const label = getOrCreateGmailLabel_(CONFIG.PROCESSED_LABEL_NAME);

    const query =
      "from:" + CONFIG.EXPECTED_SENDER_EMAIL +
      " newer_than:" + CONFIG.LIVE_NEWER_THAN_DAYS + "d" +
      " -label:" + CONFIG.PROCESSED_LABEL_NAME;

    slogInfo_(runId, "GMAIL_QUERY", "LIVE: поиск в Gmail", { query });

    const threads = GmailApp.search(query);
    slogInfo_(runId, "GMAIL_RESULT", "LIVE: найдено тредов", { threadsFound: threads.length });

    if (!threads || threads.length === 0) {
      slogOk_(runId, "NO_MAIL", "LIVE: писем не найдено", {});
      sheetLog_(runId, "LIVE", "NO_MAIL", "Писем не найдено (проверка выполнена)", {});
      return;
    }

    let processedThreads = 0;

    for (let i = 0; i < threads.length; i++) {
      const thread = threads[i];
      const threadId = thread.getId();

      const messages = thread.getMessages();
      const message = messages[messages.length - 1];

      const receivedAt = message.getDate();
      const subject = message.getSubject();
      const gmailLink = buildGmailThreadLink_(threadId);

      slogBrief_(runId, "THREAD", `threadId=${threadId} receivedAt=${receivedAt.toISOString()} subj="${truncate_(subject, 60)}"`);

      const mail = {
        mode: "LIVE",
        receivedAt,
        subject,
        threadId,
        gmailLink,
        meta: { gmailMessageLink: buildGmailMessageLink_(message) }
      };

      createTwoEventsForMail_(runId, mail);

      thread.addLabel(label);
      slogVerbose_(runId, "THREAD_LABELED", "LIVE: тред помечен PROCESSED", {
        threadId,
        label: CONFIG.PROCESSED_LABEL_NAME
      });

      processedThreads++;
    }

    slogOk_(runId, "DONE_LIVE", "LIVE завершён", { processedThreads });
    sheetLog_(runId, "LIVE", "DONE", "Live завершён", { processedThreads });

  } catch (err) {
    const payload = { error: String(err), stack: err && err.stack ? String(err.stack) : "" };
    slogErr_(runId, "FATAL", "Фатальная ошибка", payload);
    sheetLog_(runId, CONFIG.TEST_MODE ? "TEST" : "LIVE", "ERR_FATAL", "Фатальная ошибка", payload);
    throw err;
  } finally {
    lock.releaseLock();
  }
}

/***********************
 * TRIGGER SETUP
 ***********************/
function setupTriggerEveryMinute() {
  ScriptApp.newTrigger("checkMailAndCreateTwoEvents")
    .timeBased()
    .everyMinutes(1)
    .create();
}

function setupSingleTriggerEveryMinute() {
  const triggers = ScriptApp.getProjectTriggers();
  for (let i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === "checkMailAndCreateTwoEvents") {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }

  setupTriggerEveryMinute();
}

/***********************
 * SHEETS LOG UI (optional)
 ***********************/
function setupLogSheetUi() {
  const sheet = getOrCreateLogSheet_();
  setupConditionalFormatting_(sheet);
}

/***********************
 * TEST HELPERS
 ***********************/
function runTestOnce() {
  const saved = CONFIG.TEST_MODE;
  CONFIG.TEST_MODE = true;
  checkMailAndCreateTwoEvents();
  CONFIG.TEST_MODE = saved;
}

/***********************
 * DELETE TEST EVENTS
 ***********************/

/**
 * Удаляет НОВЫЕ тестовые события (созданные ЭТОЙ версией кода)
 * по строке "MAILALERT_MODE: TEST".
 */
function deleteAllTestAlerts() {
  const runId = newRunId_();
  slogInfo_(runId, "DEL_TEST_START", "Удаление TEST-событий (NEW FORMAT)", {});

  const cal = CalendarApp.getDefaultCalendar();
  const from = new Date(Date.now() - 180 * 24 * 60 * 60 * 1000);
  const to   = new Date(Date.now() + 180 * 24 * 60 * 60 * 1000);

  const events = cal.getEvents(from, to);

  let scanned = 0;
  let matched = 0;
  let deleted = 0;

  for (let i = 0; i < events.length; i++) {
    scanned++;
    const ev = events[i];
    const desc = ev.getDescription() || "";
    if (desc.indexOf("MAILALERT_MODE: TEST") === -1) continue;

    matched++;
    const title = ev.getTitle();
    const id = extractLineValue_(desc, "MAILALERT_ID");
    const kind = extractLineValue_(desc, "MAILALERT_KIND");

    slogVerbose_(runId, "DEL_TEST_MATCH", "Удаляем TEST событие", {
      title, kind, id,
      start: ev.getStartTime().toString(),
      allDay: safeIsAllDay_(ev)
    });

    ev.deleteEvent();
    deleted++;
  }

  slogOk_(runId, "DEL_TEST_DONE", "Удаление TEST (NEW FORMAT) завершено", { scanned, matched, deleted });
  sheetLog_(runId, "TEST", "DELETED", "Удалены TEST события (NEW FORMAT)", { scanned, matched, deleted });
}

/**
 * Удаляет СТАРЫЕ тестовые события (LEGACY), которые у тебя сейчас в календаре,
 * по строке "Mode: TEST" и "Expected sender: ...".
 *
 * Это нужно один раз, чтобы вычистить старые события, созданные прошлым кодом.
 */
function deleteAllTestAlerts_Legacy() {
  const runId = newRunId_();
  slogInfo_(runId, "DEL_LEGACY_START", "Удаление TEST-событий (LEGACY FORMAT: Mode: TEST)", {});

  const cal = CalendarApp.getDefaultCalendar();
  const from = new Date(Date.now() - 180 * 24 * 60 * 60 * 1000);
  const to   = new Date(Date.now() + 180 * 24 * 60 * 60 * 1000);

  const events = cal.getEvents(from, to);

  let scanned = 0;
  let matched = 0;
  let deleted = 0;

  for (let i = 0; i < events.length; i++) {
    scanned++;
    const ev = events[i];
    const desc = ev.getDescription() || "";

    // Твой старый формат:
    // Mode: TEST
    // Expected sender: queue-mailer@kdmid.ru
    if (desc.indexOf("\nMode: TEST\n") === -1) continue;
    if (desc.indexOf("\nExpected sender: " + CONFIG.EXPECTED_SENDER_EMAIL + "\n") === -1) continue;

    matched++;
    const title = ev.getTitle();

    slogVerbose_(runId, "DEL_LEGACY_MATCH", "Удаляем legacy TEST событие", {
      title,
      start: ev.getStartTime().toString(),
      allDay: safeIsAllDay_(ev)
    });

    ev.deleteEvent();
    deleted++;
  }

  slogOk_(runId, "DEL_LEGACY_DONE", "Удаление TEST (LEGACY FORMAT) завершено", { scanned, matched, deleted });
  sheetLog_(runId, "TEST", "DELETED", "Удалены TEST события (LEGACY FORMAT)", { scanned, matched, deleted });
}

/***********************
 * CORE: CREATE EVENTS
 ***********************/
function createTwoEventsForMail_(runId, mail) {
  const mode = mail.mode || "LIVE";

  // В LIVE: время письма реальное, в TEST: now
  const receivedAt = mail.receivedAt;
  const expiresAt = new Date(receivedAt.getTime() + CONFIG.ACTIVE_WINDOW_HOURS * 60 * 60 * 1000);

  // ALLDAY — на дате receivedAt
  const todayStart = startOfDay_(receivedAt);
  const tomorrowStart = addDays_(todayStart, 1);

  const now = new Date();
  const longStart = addMinutes_(now, CONFIG.EVENT1_START_PLUS_MINUTES);

  const cal = CalendarApp.getDefaultCalendar();

  // Уникальная база ID для антидубликатов:
  // В LIVE и TEST по-разному.
  // В TEST: чтобы каждый тест создавал новый комплект — используем timeKey по текущей минуте.
  // В LIVE: можно использовать receivedAt minute + threadId.
  const baseId = buildBaseId_(mode, mail.threadId || "NO_THREAD", receivedAt, now);

  slogVerbose_(runId, "ALERT_PLAN", "План событий", {
    mode,
    baseId,
    receivedAt: receivedAt.toString(),
    expiresAt: expiresAt.toString(),
    now: now.toString(),
    longStart: longStart.toString(),
    quiet: { startHour: CONFIG.QUIET_HOUR_START, endHour: CONFIG.QUIET_HOUR_END, setToHour: CONFIG.QUIET_SET_TO_HOUR },
    meta: mail.meta || {}
  });

  const subjectShort = truncate_(mail.subject || "(без темы)", 80);
  const prefix = "[" + CONFIG.EVENT_PREFIX + "][" + mode + "]";

  // -------------------------
  // EVENT 1: ALLDAY
  // -------------------------
  const allDayId = baseId + "|ALLDAY";
  const allDayTitle = prefix + " 📌 Сегодня: " + subjectShort;

  const allDayDescription = buildDescriptionNew_({
    id: allDayId,
    mode,
    kind: "ALLDAY",
    expectedSender: CONFIG.EXPECTED_SENDER_EMAIL,
    inbox: CONFIG.YOUR_INBOX_EMAIL,
    subject: mail.subject || "",
    receivedAt,
    expiresAt,
    gmailLink: mail.gmailLink || "",
    threadId: mail.threadId || "",
    meta: mail.meta || {}
  });

  // Ищем ALLDAY в диапазоне дня
  const allDayExists = findEventById_(cal, todayStart, tomorrowStart, allDayId, { allowAllDay: true });

  if (allDayExists) {
    slogOk_(runId, "ALLDAY_EXISTS", "ALLDAY уже есть (по ID)", { title: allDayExists.getTitle(), id: allDayId });
  } else {
    const ev = cal.createAllDayEvent(allDayTitle, todayStart, { description: allDayDescription });
    ev.removeAllReminders();
    ev.addPopupReminder(0); // 00:00 этого дня
    slogOk_(runId, "ALLDAY_CREATED", "Создан ALLDAY", { title: allDayTitle, id: allDayId });
  }

  // -------------------------
  // EVENT 2: LONG (24h)
  // -------------------------
  const longId = baseId + "|LONG";
  const longTitle = prefix + " 🔔 Сутки: " + truncate_(mail.subject || "(без темы)", 55);

  const longDescription = buildDescriptionNew_({
    id: longId,
    mode,
    kind: "LONG",
    expectedSender: CONFIG.EXPECTED_SENDER_EMAIL,
    inbox: CONFIG.YOUR_INBOX_EMAIL,
    subject: mail.subject || "",
    receivedAt,
    expiresAt,
    gmailLink: mail.gmailLink || "",
    threadId: mail.threadId || "",
    reminders: CONFIG.EVENT1_REMINDERS_MINUTES.join(", "),
    meta: Object.assign({}, mail.meta || {}, { longStart: longStart.toISOString() })
  });

  const longExists = findEventById_(cal, addMinutes_(longStart, -60), addMinutes_(expiresAt, 60), longId, { allowAllDay: false });
  if (longExists) {
    slogOk_(runId, "LONG_EXISTS", "LONG уже есть (по ID)", { title: longExists.getTitle(), id: longId });
  } else {
    const evLong = cal.createEvent(longTitle, longStart, expiresAt, { description: longDescription });
    evLong.removeAllReminders();
    for (let i = 0; i < CONFIG.EVENT1_REMINDERS_MINUTES.length; i++) {
      evLong.addPopupReminder(CONFIG.EVENT1_REMINDERS_MINUTES[i]);
    }
    slogOk_(runId, "LONG_CREATED", "Создано LONG (24h)", {
      title: longTitle,
      id: longId,
      longStart: longStart.toString(),
      expiresAt: expiresAt.toString(),
      remindersMinutes: CONFIG.EVENT1_REMINDERS_MINUTES
    });
  }

  // -------------------------
  // EVENT 3: HOURLY CHAIN + FINAL
  // -------------------------
  startHourlyChainForMail_(runId, mail, baseId, longStart, expiresAt);

  sheetLog_(runId, mode, "ALERTS_CREATED", "Созданы: ALLDAY + LONG + CHAIN", {
    baseId,
    allDayId,
    longId,
    receivedAt: receivedAt.toISOString(),
    longStart: longStart.toISOString(),
    expiresAt: expiresAt.toISOString()
  });
}

/***********************
 * TEST: find latest mail from sender
 ***********************/
function findLatestMailFromSender_(runId, senderEmail, lookbackDays) {
  try {
    const query = `from:${senderEmail} newer_than:${lookbackDays}d`;
    slogVerbose_(runId, "TEST_GMAIL_QUERY", "TEST: поиск последнего письма", { query });

    const threads = GmailApp.search(query, 0, 10);
    slogVerbose_(runId, "TEST_GMAIL_THREADS", "TEST: тредов найдено", { threadsFound: threads.length });

    if (!threads || threads.length === 0) return null;

    let best = null;

    for (let i = 0; i < threads.length; i++) {
      const thread = threads[i];
      const threadId = thread.getId();
      const msgs = thread.getMessages();
      const msg = msgs[msgs.length - 1];
      const d = msg.getDate();

      if (!best || d.getTime() > best.realReceivedAt.getTime()) {
        best = {
          threadId,
          subject: msg.getSubject(),
          realReceivedAt: d,
          messageId: msg.getId(),
          gmailLink: buildGmailThreadLink_(threadId),
          gmailMessageLink: buildGmailMessageLink_(msg)
        };
      }
    }

    slogInfo_(runId, "TEST_LATEST_PICKED", "TEST: выбрано последнее письмо", {
      threadId: best.threadId,
      subject: best.subject,
      realReceivedAt: best.realReceivedAt.toString(),
      gmailLink: best.gmailLink
    });

    return best;
  } catch (err) {
    slogErr_(runId, "TEST_LATEST_ERR", "TEST: ошибка поиска последнего письма", {
      error: String(err),
      stack: err && err.stack ? String(err.stack) : ""
    });
    return null;
  }
}

/***********************
 * FIND EVENT BY ID (anti-duplicate)
 ***********************/
function findEventById_(cal, from, to, alertId, opts) {
  const allowAllDay = opts && typeof opts.allowAllDay === "boolean" ? opts.allowAllDay : true;
  const events = cal.getEvents(from, to);

  for (let i = 0; i < events.length; i++) {
    const ev = events[i];

    if (!allowAllDay && safeIsAllDay_(ev)) {
      continue;
    }

    const desc = ev.getDescription() || "";
    if (desc.indexOf("MAILALERT_ID: " + alertId) !== -1) {
      return ev;
    }
  }
  return null;
}

function safeIsAllDay_(ev) {
  try { return ev.isAllDayEvent(); } catch (e) { return false; }
}

function extractLineValue_(desc, key) {
  const re = new RegExp("^" + key + ":\\s*(.*)$", "m");
  const m = desc.match(re);
  return m ? (m[1] || "").trim() : "";
}

// Округление вверх до заданного шага минут
function ceilToMinutes_(dt, stepMinutes) {
  const ms = dt.getTime();
  const stepMs = stepMinutes * 60 * 1000;
  const roundedMs = Math.ceil(ms / stepMs) * stepMs;
  return new Date(roundedMs);
}

function ceilToHour_(dt) {
  const rounded = ceilToMinutes_(dt, 60);
  return rounded;
}

function isInQuietHours_(dt) {
  const h = dt.getHours();
  return (h >= CONFIG.QUIET_HOUR_START) || (h < CONFIG.QUIET_HOUR_END);
}

function nextQuietEndToMorning_(dt) {
  const h = dt.getHours();
  const res = new Date(dt.getTime());

  if (h >= CONFIG.QUIET_HOUR_START) {
    res.setDate(res.getDate() + 1);
  }
  res.setHours(CONFIG.QUIET_SET_TO_HOUR, 0, 0, 0);
  return res;
}

/***********************
 * ID BUILDING
 ***********************/
function buildBaseId_(mode, threadId, receivedAt, now) {
  const tid8 = String(threadId || "NO_THREAD").slice(-8);

  // minute key:
  // LIVE: можно стабильно от receivedAt
  // TEST: хотим независимый запуск — используем now
  const keyDate = mode === "TEST" ? now : receivedAt;

  const y = keyDate.getFullYear();
  const m = String(keyDate.getMonth() + 1).padStart(2, "0");
  const d = String(keyDate.getDate()).padStart(2, "0");
  const hh = String(keyDate.getHours()).padStart(2, "0");
  const mm = String(keyDate.getMinutes()).padStart(2, "0");

  // TEST|YYYYMMDDHHMM|Txxxxxxxx
  return `${mode}|${y}${m}${d}${hh}${mm}|T${tid8}`;
}

/***********************
 * DESCRIPTION (NEW FORMAT)
 ***********************/
function buildDescriptionNew_(p) {
  let metaBlock = "";
  try {
    const meta = p.meta || {};
    metaBlock = Object.keys(meta).length ? ("\nMETA:\n" + JSON.stringify(meta, null, 2) + "\n") : "";
  } catch (e) {
    metaBlock = "";
  }

  const messageLink = (p.meta && p.meta.gmailMessageLink) ? String(p.meta.gmailMessageLink) : "";
  const openMailBlock = messageLink
    ? ("\nOpen mail (thread):\n" + (p.gmailLink || "") + "\n\n" +
       "Open mail (message):\n" + messageLink + "\n\n")
    : ("\nOpen mail:\n" + (p.gmailLink || "") + "\n\n");

  return (
    "MAILALERT_ID: " + p.id + "\n" +
    "MAILALERT_MODE: " + p.mode + "\n" +
    "MAILALERT_KIND: " + p.kind + "\n" +
    "Expected sender: " + (p.expectedSender || "") + "\n" +
    "Inbox: " + (p.inbox || "") + "\n" +
    "ThreadId: " + (p.threadId || "") + "\n" +
    "Subject: " + (p.subject || "") + "\n" +
    "ReceivedAt: " + (p.receivedAt ? p.receivedAt.toString() : "") + "\n" +
    "ExpiresAt: " + (p.expiresAt ? p.expiresAt.toString() : "") + "\n" +
    (p.tailStart ? ("TailStart: " + p.tailStart + "\n") : "") +
    (p.reminders ? ("Reminders (min): " + p.reminders + "\n") : "") +
    metaBlock +
    openMailBlock +
    "Удалишь событие — значит письмо обработано."
  );
}

/***********************
 * SHEETS LOG
 ***********************/
function withRetries_(label, fn, opts) {
  const attempts = opts && opts.attempts ? opts.attempts : 3;
  const baseDelayMs = opts && opts.baseDelayMs ? opts.baseDelayMs : 500;
  let lastErr = null;

  for (let i = 0; i < attempts; i++) {
    try {
      return fn();
    } catch (err) {
      lastErr = err;
      console.error(`[retry] ${label} failed (attempt ${i + 1}/${attempts}): ${err}`);
      if (i < attempts - 1) {
        Utilities.sleep(baseDelayMs * Math.pow(2, i));
      }
    }
  }
  throw lastErr;
}

function getOrCreateLogSheet_() {
  return withRetries_("getOrCreateLogSheet_", function () {
    let ss;
    const files = DriveApp.getFilesByName(CONFIG.LOG_SPREADSHEET_NAME);
    if (files.hasNext()) ss = SpreadsheetApp.open(files.next());
    else ss = SpreadsheetApp.create(CONFIG.LOG_SPREADSHEET_NAME);

    let sheet = ss.getSheetByName(CONFIG.LOG_SHEET_NAME);
    if (!sheet) sheet = ss.insertSheet(CONFIG.LOG_SHEET_NAME);

    if (sheet.getLastRow() === 0) {
      sheet.appendRow(["Timestamp", "RunId", "Mode", "Status", "Message", "JSON"]);
    }
    return sheet;
  });
}

function sheetLog_(runId, mode, status, message, obj) {
  try {
    const sheet = getOrCreateLogSheet_();
    let json = "";
    try { json = JSON.stringify(obj || {}); } catch (e) { json = String(obj); }
    const row = [new Date(), runId, mode, status, message, json];
    withRetries_("sheet.appendRow", function () { sheet.appendRow(row); });
  } catch (err) {
    console.error(`[${runId}] ERR SHEET_LOG: ${err}`);
  }
}

function setupConditionalFormatting_(sheet) {
  const lastRow = 20000;
  const rangeAll = sheet.getRange("A2:F" + lastRow);

  const rules = [];
  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenFormulaSatisfied('=LEFT($D2,4)="ERR_"')
      .setBackground("#F8CBAD")
      .setFontColor("#9C0006")
      .setRanges([rangeAll])
      .build()
  );
  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenFormulaSatisfied('=$D2="ALERTS_CREATED"')
      .setBackground("#C6EFCE")
      .setFontColor("#006100")
      .setRanges([rangeAll])
      .build()
  );
  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenFormulaSatisfied('=$D2="NO_MAIL"')
      .setBackground("#E7E7E7")
      .setFontColor("#333333")
      .setRanges([rangeAll])
      .build()
  );
  sheet.setConditionalFormatRules(rules);
}

/***********************
 * SCRIPT LOG (Executions → Logs)
 ***********************/
function slogBrief_(runId, step, line) {
  if (CONFIG.SCRIPT_LOG_LEVEL !== "brief" && CONFIG.SCRIPT_LOG_LEVEL !== "verbose") return;
  console.log(`[${runId}] ${step}: ${line}`);
}

function slogVerbose_(runId, step, message, obj) {
  if (CONFIG.SCRIPT_LOG_LEVEL !== "verbose") return;
  let json = "";
  try { json = JSON.stringify(obj || {}); } catch (e) { json = String(obj); }
  console.log(`[${runId}] ${step}: ${message} | ${json}`);
}

function slogInfo_(runId, step, message, obj) {
  if (CONFIG.SCRIPT_LOG_LEVEL === "brief") {
    console.log(`[${runId}] ${step}: ${message}`);
    return;
  }
  if (CONFIG.SCRIPT_LOG_LEVEL === "verbose") {
    slogVerbose_(runId, step, message, obj);
  }
}

function slogOk_(runId, step, message, obj) {
  if (CONFIG.SCRIPT_LOG_LEVEL === "brief") {
    console.log(`[${runId}] OK ${step}: ${message}`);
    return;
  }
  if (CONFIG.SCRIPT_LOG_LEVEL === "verbose") {
    slogVerbose_(runId, "OK " + step, message, obj);
  }
}

function slogErr_(runId, step, message, obj) {
  let json = "";
  try { json = JSON.stringify(obj || {}); } catch (e) { json = String(obj); }
  console.error(`[${runId}] ERR ${step}: ${message} | ${json}`);
}

/***********************
 * GMAIL LABEL
 ***********************/
function getOrCreateGmailLabel_(labelName) {
  let label = GmailApp.getUserLabelByName(labelName);
  if (!label) label = GmailApp.createLabel(labelName);
  return label;
}

function buildGmailThreadLink_(threadId) {
  if (threadId) return "https://mail.google.com/mail/u/0/#inbox/" + threadId;
  return "https://mail.google.com/mail/u/0/#inbox";
}

function buildGmailMessageLink_(message) {
  try {
    const header = message.getHeader("Message-ID");
    if (header) {
      const clean = String(header).replace(/[<>]/g, "");
      return "https://mail.google.com/mail/u/0/#search/rfc822msgid:" + encodeURIComponent(clean);
    }
  } catch (e) {}

  try {
    const id = message.getId();
    if (id) return "https://mail.google.com/mail/u/0/#inbox/" + id;
  } catch (e) {}

  return "https://mail.google.com/mail/u/0/#inbox";
}

/***********************
 * CHAIN: HOURLY EVENTS
 ***********************/
function startHourlyChainForMail_(runId, mail, baseId, longStart, expiresAt) {
  const chainId = baseId + "|HCHAIN";

  if (getHourlyChainState_(chainId)) {
    slogOk_(runId, "HCHAIN_EXISTS", "HOURLY-CHAIN уже запущена", { chainId });
    return;
  }

  const signals = buildHourlySignals_(longStart, expiresAt);
  if (!signals.length) {
    slogOk_(runId, "HCHAIN_SKIP", "HOURLY-CHAIN не нужна: нет сигналов", {});
    return;
  }

  const blocks = buildHourlyBlocks_(signals);
  if (!blocks.length) {
    slogOk_(runId, "HCHAIN_SKIP", "HOURLY-CHAIN не нужна: нет блоков", {});
    return;
  }

  const first = blocks[0];
  const ctx = {
    mode: mail.mode || "LIVE",
    subject: mail.subject || "",
    threadId: mail.threadId || "",
    gmailLink: mail.gmailLink || "",
    meta: mail.meta || {},
    receivedAt: mail.receivedAt,
    expiresAt: expiresAt.toISOString()
  };
  const eventId = createHourlyBlockEvent_(ctx, chainId, 0, first);

  const state = {
    chainId,
    baseId,
    mode: mail.mode || "LIVE",
    subject: mail.subject || "",
    threadId: mail.threadId || "",
    gmailLink: mail.gmailLink || "",
    meta: mail.meta || {},
    receivedAt: mail.receivedAt.toISOString(),
    longStart: longStart.toISOString(),
    expiresAt: expiresAt.toISOString(),
    blocks,
    indexCurrent: 0,
    currentEventId: eventId,
    pendingDeleteEventId: "",
    status: "active"
  };

  saveHourlyChainState_(state);
  ensureHourlyChainTrigger_();

  slogOk_(runId, "HCHAIN_STARTED", "HOURLY-CHAIN: создан первый блок", {
    chainId,
    eventId,
    start: first.start
  });
}

function processHourlyChains_() {
  const now = new Date();
  const cal = CalendarApp.getDefaultCalendar();
  const chains = listHourlyChainStates_();
  if (!chains.length) {
    cleanupHourlyChainTrigger_();
    return;
  }

  for (let i = 0; i < chains.length; i++) {
    const st = chains[i];

    if (st.pendingDeleteEventId) {
      const pending = findEventById_(cal, addHours_(now, -48), addHours_(now, 48), st.pendingDeleteEventId, { allowAllDay: false });
      if (pending) pending.deleteEvent();
      st.pendingDeleteEventId = "";
    }

    if (st.status === "final_cleanup") {
      deleteHourlyChainState_(st.chainId);
      continue;
    }

    if (!st.currentEventId) {
      deleteHourlyChainState_(st.chainId);
      continue;
    }

    const currentBlock = st.blocks[st.indexCurrent];
    const exists = findEventById_(cal, addHours_(now, -48), addHours_(now, 48), st.currentEventId, { allowAllDay: false });

    if (!exists) {
      deleteHourlyChainState_(st.chainId);
      continue;
    }

    const currentStart = new Date(currentBlock.start);
    if (now.getTime() < currentStart.getTime()) {
      saveHourlyChainState_(st);
      continue;
    }

    // Если есть следующий блок — создаём его
    if (st.indexCurrent < st.blocks.length - 1) {
      const nextIndex = st.indexCurrent + 1;
      const nextBlock = st.blocks[nextIndex];
      const nextEventId = createHourlyBlockEvent_(st, st.chainId, nextIndex, nextBlock);

      st.pendingDeleteEventId = st.currentEventId;
      st.currentEventId = nextEventId;
      st.indexCurrent = nextIndex;
      saveHourlyChainState_(st);
      continue;
    }

    // Последний блок: создаём финальное событие №3
    const finalEventId = createFinalEvent_(st);
    if (finalEventId) {
      st.pendingDeleteEventId = st.currentEventId;
      st.currentEventId = "";
      st.status = "final_cleanup";
      saveHourlyChainState_(st);
    } else {
      deleteHourlyChainState_(st.chainId);
    }
  }
}

function buildHourlySignals_(longStart, expiresAt) {
  const endSignal = addHours_(expiresAt, -1);
  const firstRaw = addHours_(longStart, 1);
  let t = ceilToHour_(firstRaw);

  const signals = [];
  while (t.getTime() <= endSignal.getTime()) {
    if (!isInQuietHours_(t)) signals.push(new Date(t));
    t = addHours_(t, CONFIG.HOURLY_INTERVAL_HOURS);
  }

  if (!isInQuietHours_(endSignal)) {
    if (!signals.length || signals[signals.length - 1].getTime() !== endSignal.getTime()) {
      signals.push(endSignal);
    }
  }

  return signals;
}

function buildHourlyBlocks_(signals) {
  const blocks = [];
  const step = CONFIG.HOURLY_BLOCK_MAX_SIGNALS;

  for (let i = 0; i < signals.length; i += step) {
    const slice = signals.slice(i, i + step);
    if (!slice.length) continue;
    const start = slice[slice.length - 1];
    const reminders = [];
    for (let j = 0; j < slice.length; j++) {
      const diffMin = Math.round((start.getTime() - slice[j].getTime()) / 60000);
      reminders.push(diffMin);
    }
    blocks.push({
      start: start.toISOString(),
      reminders
    });
  }

  return blocks;
}

function createHourlyBlockEvent_(mailOrState, chainId, index, block) {
  const cal = CalendarApp.getDefaultCalendar();
  const start = new Date(block.start);
  const end = addMinutes_(start, 5);
  const eventId = chainId + "|B" + index + "|" + formatYYYYMMDDHHMM_(start);
  const mode = mailOrState.mode || "LIVE";
  const subject = mailOrState.subject || "";
  const threadId = mailOrState.threadId || "";
  const gmailLink = mailOrState.gmailLink || "";
  const meta = mailOrState.meta || {};

  const title = "[" + CONFIG.EVENT_PREFIX + "][" + mode + "] 🔔 Часовые";
  const desc = buildDescriptionNew_({
    id: eventId,
    mode,
    kind: "HOURLY",
    expectedSender: CONFIG.EXPECTED_SENDER_EMAIL,
    inbox: CONFIG.YOUR_INBOX_EMAIL,
    subject,
    receivedAt: mailOrState.receivedAt ? new Date(mailOrState.receivedAt) : "",
    expiresAt: mailOrState.expiresAt ? new Date(mailOrState.expiresAt) : "",
    gmailLink,
    threadId,
    reminders: block.reminders.join(", "),
    meta: Object.assign({}, meta, { chainId, blockIndex: index })
  });

  const ev = cal.createEvent(title, start, end, { description: desc });
  ev.removeAllReminders();
  for (let i = 0; i < block.reminders.length; i++) {
    ev.addPopupReminder(block.reminders[i]);
  }
  return eventId;
}

function createFinalEvent_(state) {
  const cal = CalendarApp.getDefaultCalendar();
  const endAt = new Date(state.expiresAt);
  const start = endAt;
  const end = addMinutes_(start, 5);
  const eventId = state.chainId + "|FINAL|" + formatYYYYMMDDHHMM_(start);

  const title = "[" + CONFIG.EVENT_PREFIX + "][" + state.mode + "] ❗ Финал";
  const desc = buildDescriptionNew_({
    id: eventId,
    mode: state.mode,
    kind: "FINAL",
    expectedSender: CONFIG.EXPECTED_SENDER_EMAIL,
    inbox: CONFIG.YOUR_INBOX_EMAIL,
    subject: state.subject,
    receivedAt: state.receivedAt ? new Date(state.receivedAt) : "",
    expiresAt: endAt,
    gmailLink: state.gmailLink || "",
    threadId: state.threadId || "",
    reminders: CONFIG.FINAL_REMINDERS_MINUTES.join(", "),
    meta: Object.assign({}, state.meta || {}, { chainId: state.chainId })
  });

  const ev = cal.createEvent(title, start, end, { description: desc });
  ev.removeAllReminders();
  for (let i = 0; i < CONFIG.FINAL_REMINDERS_MINUTES.length; i++) {
    ev.addPopupReminder(CONFIG.FINAL_REMINDERS_MINUTES[i]);
  }
  return eventId;
}

function ensureHourlyChainTrigger_() {
  const triggers = ScriptApp.getProjectTriggers();
  let has = false;
  for (let i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === "processHourlyChains_") {
      has = true;
      break;
    }
  }
  if (has) return;

  ScriptApp.newTrigger("processHourlyChains_")
    .timeBased()
    .everyHours(1)
    .create();
}

function cleanupHourlyChainTrigger_() {
  const chains = listHourlyChainStates_();
  if (chains.length) return;
  const triggers = ScriptApp.getProjectTriggers();
  for (let i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === "processHourlyChains_") {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }
}

function saveHourlyChainState_(state) {
  PropertiesService.getScriptProperties().setProperty("MAILALERT_HCHAIN:" + state.chainId, JSON.stringify(state));
}

function getHourlyChainState_(chainId) {
  const raw = PropertiesService.getScriptProperties().getProperty("MAILALERT_HCHAIN:" + chainId);
  if (!raw) return null;
  try { return JSON.parse(raw); } catch (e) { return null; }
}

function deleteHourlyChainState_(chainId) {
  PropertiesService.getScriptProperties().deleteProperty("MAILALERT_HCHAIN:" + chainId);
}

function listHourlyChainStates_() {
  const props = PropertiesService.getScriptProperties().getProperties();
  const keys = Object.keys(props).filter(k => k.indexOf("MAILALERT_HCHAIN:") === 0);
  const out = [];
  for (let i = 0; i < keys.length; i++) {
    try { out.push(JSON.parse(props[keys[i]])); } catch (e) {}
  }
  return out;
}

/***********************
 * DATE HELPERS
 ***********************/
function startOfDay_(dt) {
  return new Date(dt.getFullYear(), dt.getMonth(), dt.getDate(), 0, 0, 0);
}

function addDays_(dt, days) {
  const d = new Date(dt.getTime());
  d.setDate(d.getDate() + days);
  return d;
}

function addHours_(dt, hours) {
  return addMinutes_(dt, hours * 60);
}

function addMinutes_(dt, minutes) {
  const d = new Date(dt.getTime());
  d.setMinutes(d.getMinutes() + minutes);
  return d;
}

function truncate_(s, n) {
  if (!s) return "";
  return s.length <= n ? s : s.slice(0, n - 1) + "…";
}

function formatYYYYMMDDHHMM_(dt) {
  const y = dt.getFullYear();
  const m = String(dt.getMonth() + 1).padStart(2, "0");
  const d = String(dt.getDate()).padStart(2, "0");
  const hh = String(dt.getHours()).padStart(2, "0");
  const mm = String(dt.getMinutes()).padStart(2, "0");
  return `${y}${m}${d}${hh}${mm}`;
}

/***********************
 * RUN ID
 ***********************/
function newRunId_() {
  return Utilities.getUuid().slice(0, 8);
}
