/*************************************************************
 * PROJECT: Mail → Calendar “НЕ ПРОЗЕВАТЬ”
 *
 * Цель:
 *  - ждать письмо от queue-mailer@kdmid.ru (действительно 24 часа)
 *  - создать в Google Calendar события, чтобы сложно было пропустить уведомления
 *
 * Модель: 2 события
 *  1) ALLDAY (сегодня) — визуальный якорь “как ДР”
 *  2) TAIL (боевое) — “пищит”, стартует скоро (now+2мин, округление до 5 минут),
 *     но если попало в тихий интервал 23:00–07:00 → старт 07:00.
 *     end = receivedAt + 24 часа.
 *     reminders: 0, +2ч, +4ч, +6ч, +8ч.
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

  // Напоминания для tail-события (минуты от НАЧАЛА tail)
  TAIL_REMINDERS_MINUTES: [0, 120, 240, 360, 480],

  // Tail start: now + 2 минуты, округление вверх до 5 минут
  TAIL_START_PLUS_MINUTES: 2,
  TAIL_START_ROUND_MINUTES: 5,

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
      const gmailLink = "https://mail.google.com/mail/u/0/#inbox/" + threadId;

      slogBrief_(runId, "THREAD", `threadId=${threadId} receivedAt=${receivedAt.toISOString()} subj="${truncate_(subject, 60)}"`);

      const mail = {
        mode: "LIVE",
        receivedAt,
        subject,
        threadId,
        gmailLink,
        meta: {}
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
 * CORE: CREATE 2 EVENTS
 ***********************/
function createTwoEventsForMail_(runId, mail) {
  const mode = mail.mode || "LIVE";

  // В LIVE: время письма реальное, в TEST: now
  const receivedAt = mail.receivedAt;
  const expiresAt = new Date(receivedAt.getTime() + CONFIG.ACTIVE_WINDOW_HOURS * 60 * 60 * 1000);

  // ALLDAY — на дате receivedAt
  const todayStart = startOfDay_(receivedAt);
  const tomorrowStart = addDays_(todayStart, 1);

  // TAIL — старт “скоро” от текущего now (а не от receivedAt), чтобы пищало сразу после запуска
  const now = new Date();
  const tailStartCandidate = computeTailStart_(now);

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
    tailStartCandidate: tailStartCandidate.toString(),
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
  // EVENT 2: TAIL
  // -------------------------
  const tailStart = tailStartCandidate;
  const tailEnd = expiresAt;

  if (tailEnd.getTime() <= tailStart.getTime()) {
    slogOk_(runId, "TAIL_SKIP", "TAIL не нужен: expiresAt <= tailStart", {
      tailStart: tailStart.toString(),
      tailEnd: tailEnd.toString()
    });

    sheetLog_(runId, mode, "ALERTS_CREATED", "Создано: ALLDAY, TAIL=SKIP", {
      baseId, allDayId,
      receivedAt: receivedAt.toISOString(),
      expiresAt: expiresAt.toISOString(),
      tailStart: tailStart.toISOString()
    });
    return;
  }

  const tailId = baseId + "|TAIL";
  const tailTitle = prefix + " 🔔 Пищит: до " + formatHHMM_(tailEnd) + " — " + truncate_(mail.subject || "(без темы)", 55);

  const tailDescription = buildDescriptionNew_({
    id: tailId,
    mode,
    kind: "TAIL",
    expectedSender: CONFIG.EXPECTED_SENDER_EMAIL,
    inbox: CONFIG.YOUR_INBOX_EMAIL,
    subject: mail.subject || "",
    receivedAt,
    expiresAt,
    gmailLink: mail.gmailLink || "",
    threadId: mail.threadId || "",
    tailStart: tailStart.toString(),
    reminders: CONFIG.TAIL_REMINDERS_MINUTES.join(", "),
    meta: mail.meta || {}
  });

  // ВАЖНО: при поиске TAIL игнорируем all-day
  const tailExists = findEventById_(cal, tailStart, tailEnd, tailId, { allowAllDay: false });

  if (tailExists) {
    slogOk_(runId, "TAIL_EXISTS", "TAIL уже есть (по ID)", { title: tailExists.getTitle(), id: tailId });
  } else {
    const ev2 = cal.createEvent(tailTitle, tailStart, tailEnd, { description: tailDescription });
    ev2.removeAllReminders();
    for (let i = 0; i < CONFIG.TAIL_REMINDERS_MINUTES.length; i++) {
      ev2.addPopupReminder(CONFIG.TAIL_REMINDERS_MINUTES[i]);
    }
    slogOk_(runId, "TAIL_CREATED", "Создан TAIL + reminders", {
      title: tailTitle,
      id: tailId,
      tailStart: tailStart.toString(),
      tailEnd: tailEnd.toString(),
      remindersMinutes: CONFIG.TAIL_REMINDERS_MINUTES
    });
  }

  sheetLog_(runId, mode, "ALERTS_CREATED", "Созданы/проверены 2 события", {
    baseId,
    allDayId,
    tailId,
    receivedAt: receivedAt.toISOString(),
    expiresAt: expiresAt.toISOString(),
    tailStart: tailStart.toISOString(),
    tailEnd: tailEnd.toISOString(),
    subject: mail.subject || "",
    threadId: mail.threadId || "",
    meta: mail.meta || {}
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
          gmailLink: "https://mail.google.com/mail/u/0/#inbox/" + threadId
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

/***********************
 * TAIL START CALC
 ***********************/
function computeTailStart_(now) {
  const plusMs = CONFIG.TAIL_START_PLUS_MINUTES * 60 * 1000;
  const d = new Date(now.getTime() + plusMs);

  const rounded = ceilToMinutes_(d, CONFIG.TAIL_START_ROUND_MINUTES);

  if (isInQuietHours_(rounded)) {
    return nextQuietEndToMorning_(rounded);
  }
  return rounded;
}

function ceilToMinutes_(dt, stepMinutes) {
  const ms = dt.getTime();
  const stepMs = stepMinutes * 60 * 1000;
  const roundedMs = Math.ceil(ms / stepMs) * stepMs;
  return new Date(roundedMs);
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

function formatHHMM_(dt) {
  const hh = String(dt.getHours()).padStart(2, "0");
  const mm = String(dt.getMinutes()).padStart(2, "0");
  return `${hh}:${mm}`;
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
    "\nOpen mail:\n" + (p.gmailLink || "") + "\n\n" +
    "Удалишь событие — значит письмо обработано."
  );
}

/***********************
 * SHEETS LOG
 ***********************/
function getOrCreateLogSheet_() {
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
}

function sheetLog_(runId, mode, status, message, obj) {
  const sheet = getOrCreateLogSheet_();
  let json = "";
  try { json = JSON.stringify(obj || {}); } catch (e) { json = String(obj); }
  sheet.appendRow([new Date(), runId, mode, status, message, json]);
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

function truncate_(s, n) {
  if (!s) return "";
  return s.length <= n ? s : s.slice(0, n - 1) + "…";
}

/***********************
 * RUN ID
 ***********************/
function newRunId_() {
  return Utilities.getUuid().slice(0, 8);
}
