/************ CONFIG ************/
/*
  Идентификаторы и имена листов.
  SOURCE_SPREADSHEET_ID:
    - ID Google Таблицы-источника (строка между /d/ и /edit),
    - либо полный URL (ID извлечётся автоматически),
    - либо 'SELF', если источник — этот же файл.
*/
const SOURCE_SPREADSHEET_ID = '1PkCFO9fRC6kowEgO-TyVOhHIreOAiww4goqa6aga8pE';
const SOURCE_SHEET_NAME     = 'Лист1';       // лист-источник
const TARGET_SHEET_NAME     = 'Лист1';       // лист-приёмник (в ЭТОМ файле)
const QUEUE_SHEET_NAME      = '_QueueBuffer';// скрытый лист-очередь (в ЭТОМ файле)

// Частота записи из очереди в приёмник.
// Допустимые значения: 1, 5, 10, 15, 30 минут или кратно 60 (часовые интервалы).
const INTERVAL_MINUTES = 1;

// Размер блока чтения при прайминге (баланс скорости и лимитов).
const PRIME_READ_CHUNK_ROWS  = 1000;

/************ СХЕМА ДАННЫХ (порядок столбцов) ************/
const REQUIRED_HEADERS = [
  'Date-time',
  'Day of the week',
  'Duty cycle',
  'Period of day',
  'ID ITP',
  'Nomer ITP',
  'Adres MKD',
  'FIAS MKD',
  'UNOM MKD',
  'ID HVS ITP',
  'ID ODPU GVS',
  'ID ODPU GVS ch1',
  'ID ODPU GVS ch2',
  'Znach HVS ITP',
  'Znach ODPU GVS  ch1',
  'Znach ODPU GVS  ch2',
  'Potrebl GVS',
  'Otkl potrebl HVS-GVS',
  'Predict Otkl potrebl HVS-GVS'
];

/************ STATE/PROPS ************/
const PROP_NS = 'HACKATHON_ROW_QUEUE';
const PROP_QUEUE_NEXT = 'queueNextRow';   // следующая к выгрузке строка очереди (1-based, включая шапку)
const PROP_QUEUE_LAST = 'queueLastRow';   // последняя заполненная строка очереди (1-based)

/************ TELEGRAM CONFIG ************/
// Включение/выключение уведомлений в Telegram.
const TELEGRAM_ENABLED   = true;
// Токен бота (создаётся через @BotFather).
const TELEGRAM_BOT_TOKEN = '8229748963:AAE1-WEopOJEUYRHybJI5pXsvLu8uZb059g';
// Дополнительные chat_id из конфига (необязательно). Можно оставить пустым — будет автосбор.
const TELEGRAM_CHAT_IDS  = [
  1042557236
];
// Режим форматирования текста ('HTML' или 'MarkdownV2').
const TELEGRAM_PARSE_MODE = 'HTML';

/************ TELEGRAM AUTOREG ************/
// Автоматически подхватывать новые чаты из getUpdates.
const TELEGRAM_AUTOREGISTER = true;

// Ключи хранилища (Script Properties) для автосбора chat_id и смещения getUpdates.
const PROP_TELEGRAM_CHAT_IDS_JSON  = 'TELEGRAM_CHAT_IDS_JSON';
const PROP_TELEGRAM_LAST_UPDATE_ID = 'TELEGRAM_LAST_UPDATE_ID';

/************ ПОЛЬЗОВАТЕЛЬСКОЕ МЕНЮ (Google Sheets) ************/
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Hackathon Queue')
    .addItem('▶ Setup & Prime (заполнить очередь и создать таймер)', 'setupAndPrime')
    .addSeparator()
    .addItem('↓ Выгрузить 1 строку сейчас (тест)', 'testDequeueOnce_')
    .addSeparator()
    .addItem('🔍 Debug: состояние очереди', 'debugQueueState_')
    .addItem('🛠 Debug: починить указатели', 'fixQueuePointers_')
    .addSeparator()
    .addItem('⏱ Поставить таймер на 1 минуту', 'resetTriggerTo1Minute_')
    .addItem('⏱ Поставить таймер на 60 минут', 'resetTriggerTo60Minute_')
    .addItem('🧹 Удалить все таймеры', 'deleteAllTriggers')
    .addSeparator()
    .addItem('📄 Debug: открыть источник и проверить лист', 'debugSourceOpen_')
    .addSeparator()
    .addItem('🤖 Telegram: принудительно опросить getUpdates', 'telegramForcePoll_')
    .addItem('🤖 Telegram: показать сохранённые chat_id', 'telegramShowStoredChats_')
    .addToUi();
}

/************ ГЛАВНАЯ ИНИЦИАЛИЗАЦИЯ ************/
/*
  setupAndPrime():
  - проверяет/создаёт шапку в целевом листе,
  - собирает очередь из источника (очистив старую),
  - пересоздаёт таймер с периодичностью INTERVAL_MINUTES.
*/
function setupAndPrime() {
  ensureTargetHeaders_();
  primeQueueFromSource_({ clearQueue: true });
  ensureOrResetTrigger_('dequeueOneHourly', INTERVAL_MINUTES);
  Logger.log(`Очередь сформирована. Таймер каждые ${INTERVAL_MINUTES} мин.`);
}

/************ ВЫГРУЗКА ПО ТАЙМЕРУ (ПАЧКИ ОДНОГО Date-time) ************/
/*
  Берёт из очереди все подряд идущие строки с минимальным Date-time и
  добавляет их одним блоком в целевой лист. Для каждой строки рассчитывается Anomaly.
  При Anomaly = 1 отправляется уведомление в Telegram.
*/
function dequeueOneHourly() {
  const lock = LockService.getScriptLock();
  lock.tryLock(30 * 1000);
  try {
    const ss = SpreadsheetApp.getActive();
    const queue = getOrCreateQueueSheet_(ss);
    const targetSheet = getExistingSheet_(ss, TARGET_SHEET_NAME, true);

    let next = getIntProp_(PROP_QUEUE_NEXT, 2);
    const last = getIntProp_(PROP_QUEUE_LAST, 1);
    if (next > last) {
      Logger.log('Очередь пуста — нечего выгружать.');
      return;
    }

    // Читаем хвост очереди, начиная с next.
    const remainRows = last - next + 1;
    const remainVals = queue.getRange(next, 1, remainRows, REQUIRED_HEADERS.length).getValues();
    if (remainVals.length === 0) {
      Logger.log('Нет данных после указателя.');
      return;
    }

    // Ключ Date-time первой строки пачки (после сортировки — самый ранний).
    const dtKey0 = dateKey_(remainVals[0][0]);

    // Размер пачки = количество подряд строк с тем же Date-time.
    let batchSize = 0;
    for (let i = 0; i < remainVals.length; i++) {
      const k = dateKey_(remainVals[i][0]);
      if (k === dtKey0) batchSize++; else break;
    }
    const batch = remainVals.slice(0, batchSize);

    // Гарантируем корректную шапку/порядок у приёмника.
    ensureTargetHeaders_();

    // Записываем пачку за один вызов.
    const startWriteRow = targetSheet.getLastRow() + 1;
    targetSheet.getRange(startWriteRow, 1, batch.length, REQUIRED_HEADERS.length).setValues(batch);

    // Проставляем Anomaly для всей пачки.
    const headers = readHeaders_(targetSheet);
    const idxOtklT = headers.indexOf('Otkl potrebl HVS-GVS');
    const idxAnomT = headers.indexOf('Anomaly');
    if (idxOtklT !== -1 && idxAnomT !== -1) {
      const otklRange = targetSheet.getRange(startWriteRow, idxOtklT + 1, batch.length, 1).getValues();
      const outAnom = otklRange.map(r => [Number(r[0]) > 0.1 ? 1 : 0]);
      targetSheet.getRange(startWriteRow, idxAnomT + 1, batch.length, 1).setValues(outAnom);
    } else {
      Logger.log('Не найден "Anomaly" или "Otkl potrebl HVS-GVS" в приёмнике — отметка пропущена.');
    }

    // Отправка Telegram для строк пачки, где Anomaly = 1.
    const idxOtklQ = REQUIRED_HEADERS.indexOf('Otkl potrebl HVS-GVS');
    const idxITP   = REQUIRED_HEADERS.indexOf('ID ITP');
    const idxNum   = REQUIRED_HEADERS.indexOf('Nomer ITP');
    const idxAddr  = REQUIRED_HEADERS.indexOf('Adres MKD');
    const idxFIAS  = REQUIRED_HEADERS.indexOf('FIAS MKD');
    const idxUNOM  = REQUIRED_HEADERS.indexOf('UNOM MKD');
    const idxHVS   = REQUIRED_HEADERS.indexOf('ID HVS ITP');
    const idxGVS   = REQUIRED_HEADERS.indexOf('ID ODPU GVS');
    const idxZnachH= REQUIRED_HEADERS.indexOf('Znach HVS ITP');
    const idxPotr  = REQUIRED_HEADERS.indexOf('Potrebl GVS');

    const fileUrl = ss.getUrl();
    const gid = targetSheet.getSheetId();

    for (let i = 0; i < batch.length; i++) {
      const row = batch[i];
      const otklVal = Number(row[idxOtklQ]);
      const anomaly = otklVal > 0.1 ? 1 : 0;
      if (anomaly !== 1) continue;

      const lastRowJustWritten = startWriteRow + i;
      const link = fileUrl + '#gid=' + gid + '&range=A' + lastRowJustWritten;

      const lines = [
        '⚠️ <b>Обнаружена аномалия потребления</b>',
        '',
        '<b>Дата-время:</b> ' + escHtml_(row[0]),
        '<b>Адрес:</b> ' + escHtml_(row[idxAddr]),
        '<b>FIAS / UNOM:</b> ' + escHtml_(row[idxFIAS]) + ' / ' + escHtml_(row[idxUNOM]),
        '<b>Номер ИТП / ID ИТП:</b> ' + escHtml_(row[idxNum]) + ' / ' + escHtml_(row[idxITP]),
        '<b>ID ХВС ИТП / ID ОДПУ ГВС:</b> ' + escHtml_(row[idxHVS]) + ' / ' + escHtml_(row[idxGVS]),
        '<b>Потребление ХВС (м³):</b> ' + escHtml_(row[idxZnachH]),
        '<b>Потребление ГВС (м³):</b> ' + escHtml_(row[idxPotr]),
        '<b>Отклонение ХВС-ГВС:</b> ' + escHtml_(formatPercent_(otklVal))
      ];
      sendTelegramMessage_(lines.join('\n'));
    }

    // Сдвигаем указатель на размер записанной пачки.
    setProp_(PROP_QUEUE_NEXT, String(next + batchSize));
    Logger.log(`Выгружено строк: ${batchSize} (Date-time=${dtKey0}).`);

  } catch (e) {
    notifyError_(e);
    throw e;
  } finally {
    try { lock.releaseLock(); } catch (_) {}
  }
}

/************ ПРАЙМИНГ ОЧЕРЕДИ ************/
/*
  Собирает очередь из источника в скрытый лист _QueueBuffer.
  Все строки конвертируются к REQUIRED_HEADERS и сортируются по Date-time по возрастанию,
  чтобы в дальнейшем выгружать пачками с одинаковым Date-time.
*/
function primeQueueFromSource_({ clearQueue }) {
  const sourceSS = getSourceSpreadsheet_();
  const sourceSheet = getExistingSheet_(sourceSS, SOURCE_SHEET_NAME);

  const ss = SpreadsheetApp.getActive();
  const queue = getOrCreateQueueSheet_(ss);

  const srcHeaders = readHeaders_(sourceSheet);
  validateRequiredHeaders_(srcHeaders, 'источник');
  const headerIndexMap = toHeaderIndexMap_(srcHeaders);

  if (clearQueue) {
    queue.clear();
    queue.getRange(1, 1, 1, REQUIRED_HEADERS.length).setValues([REQUIRED_HEADERS]);
    queue.hideSheet();
  } else {
    if (queue.getLastRow() === 0) {
      queue.getRange(1, 1, 1, REQUIRED_HEADERS.length).setValues([REQUIRED_HEADERS]);
      queue.hideSheet();
    } else {
      const qh = readHeaders_(queue);
      validateRequiredHeaders_(qh, 'очередь');
    }
  }

  const lastSourceRow = sourceSheet.getLastRow();
  if (lastSourceRow <= 1) {
    setProp_(PROP_QUEUE_NEXT, '2');
    setProp_(PROP_QUEUE_LAST, '1');
    Logger.log('В источнике только шапка — очередь пуста.');
    return;
  }

  let srcStartRow = 2;
  let qWriteRow = queue.getLastRow() + 1;

  while (srcStartRow <= lastSourceRow) {
    const toRead = Math.min(PRIME_READ_CHUNK_ROWS, lastSourceRow - srcStartRow + 1);
    if (toRead <= 0) break;

    const block = sourceSheet.getRange(srcStartRow, 1, toRead, sourceSheet.getLastColumn()).getValues();

    const prepared = block.map(row => REQUIRED_HEADERS.map(h => {
      const idx = headerIndexMap[h];
      const raw = (idx != null) ? row[idx] : '';
      return (h === 'Date-time') ? parseDateTimeOrKeep_(raw) : raw;
    }));

    queue.getRange(qWriteRow, 1, prepared.length, REQUIRED_HEADERS.length).setValues(prepared);

    srcStartRow += toRead;
    qWriteRow   += prepared.length;

    SpreadsheetApp.flush();
  }

  // Сортировка данных очереди по Date-time (шапка — 1-я строка — не трогается).
  const totalRows = queue.getLastRow();
  if (totalRows > 2) {
    const dataRange = queue.getRange(2, 1, totalRows - 1, REQUIRED_HEADERS.length);
    dataRange.sort({ column: 1, ascending: true });
  }

  setProp_(PROP_QUEUE_NEXT, '2');
  setProp_(PROP_QUEUE_LAST, String(queue.getLastRow()));
  Logger.log(`Очередь сформирована и отсортирована: ${queue.getLastRow() - 1} строк (без шапки).`);
}

/************ ШАПКИ/ПОРЯДОК ************/
/*
  ensureTargetHeaders_():
  - гарантирует наличие шапки в целевом листе,
  - проверяет, что все REQUIRED_HEADERS присутствуют,
  - при необходимости переставляет колонки, чтобы упорядочить первые N столбцов по REQUIRED_HEADERS.
  Дополнительные пользовательские столбцы, добавленные справа, не удаляются.
*/
function ensureTargetHeaders_() {
  const ss = SpreadsheetApp.getActive();
  const targetSheet = getExistingSheet_(ss, TARGET_SHEET_NAME, true);

  if (targetSheet.getLastRow() === 0) {
    targetSheet.appendRow(REQUIRED_HEADERS);
    return;
  }
  const tgtHeaders = readHeaders_(targetSheet);
  validateRequiredHeaders_(tgtHeaders, 'приёмник');
  alignTargetHeaderOrderIfNeeded_(targetSheet, tgtHeaders);
}

function readHeaders_(sheet) {
  if (!sheet || sheet.getLastRow() < 1) return [];
  return sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0].map(v => String(v).trim());
}

function validateRequiredHeaders_(headers, whereLabel) {
  const set = new Set(headers.map(h => h.trim()));
  const missing = REQUIRED_HEADERS.filter(h => !set.has(h));
  if (missing.length) {
    throw new Error(`В ${whereLabel} отсутствуют обязательные заголовки: ${missing.join(', ')}`);
  }
}

function alignTargetHeaderOrderIfNeeded_(sheet, currentHeaders) {
  const desired = REQUIRED_HEADERS;
  const colCount = sheet.getLastColumn();
  if (colCount < desired.length) return;

  for (let targetPos = 0; targetPos < desired.length; targetPos++) {
    const want = desired[targetPos];
    const current = readHeaders_(sheet);
    if (current[targetPos] === want) continue;

    const fromIndex = current.indexOf(want);
    if (fromIndex === -1) continue;

    const fromCol = fromIndex + 1;
    const toCol   = targetPos + 1;
    sheet.moveColumns(sheet.getRange(1, fromCol, sheet.getMaxRows(), 1), toCol);
  }
}

/************ ТРИГГЕРЫ ************/
/*
  Удаляет все существующие триггеры указанного обработчика и создаёт новый
  с периодом INTERVAL_MINUTES. Допустимые интервалы: 1,5,10,15,30 минут или кратно 60.
*/
function ensureOrResetTrigger_(handlerName, intervalMinutes) {
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === handlerName)
    .forEach(t => ScriptApp.deleteTrigger(t));

  const minutes = Number(intervalMinutes);
  if (!Number.isFinite(minutes) || minutes <= 0) {
    throw new Error('INTERVAL_MINUTES должен быть положительным числом.');
  }

  const allowedMinuteSteps = [1, 5, 10, 15, 30];
  const tb = ScriptApp.newTrigger(handlerName).timeBased();

  if (minutes % 60 === 0) {
    const hours = Math.max(1, Math.floor(minutes / 60));
    tb.everyHours(hours).create();
    return;
  }

  if (!allowedMinuteSteps.includes(minutes)) {
    throw new Error(`Разрешены интервалы: ${allowedMinuteSteps.join(', ')} минут или кратно 60.`);
  }

  tb.everyMinutes(minutes).create();
}

/************ HELPERS (общие) ************/
function getExistingSheet_(ss, name, createIfMissing) {
  let s = ss.getSheetByName(name);
  if (!s && createIfMissing) s = ss.insertSheet(name);
  if (!s) throw new Error(`Не найден лист "${name}"`);
  return s;
}

function getOrCreateQueueSheet_(ss) {
  let q = ss.getSheetByName(QUEUE_SHEET_NAME);
  if (!q) {
    q = ss.insertSheet(QUEUE_SHEET_NAME);
    q.getRange(1, 1, 1, REQUIRED_HEADERS.length).setValues([REQUIRED_HEADERS]);
    q.hideSheet();
  }
  return q;
}

function toHeaderIndexMap_(headers) {
  const m = {};
  headers.forEach((h, i) => (m[h] = i));
  return m;
}

// Парсит строку "YYYY-MM-DD HH:mm:ss" (или с 'T') в Date; иначе возвращает исходное значение.
function parseDateTimeOrKeep_(value) {
  if (Object.prototype.toString.call(value) === '[object Date]') return value;
  if (typeof value !== 'string') return value;
  const s = value.trim().replace('T', ' ');
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})$/);
  if (!m) return value;
  const [_, Y, M, D, h, mnt, sec] = m;
  return new Date(Number(Y), Number(M) - 1, Number(D), Number(h), Number(mnt), Number(sec), 0);
}

function getIntProp_(key, defVal) {
  const sp = PropertiesService.getScriptProperties();
  const v = sp.getProperty(`${PROP_NS}.${key}`);
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : defVal;
}

function setProp_(key, val) {
  PropertiesService.getScriptProperties().setProperty(`${PROP_NS}.${key}`, val);
}

// Логирование ошибок (по желанию можно добавить e-mail/Telegram-уведомления).
function notifyError_(e) {
  Logger.log('Error: ' + (e && e.stack ? e.stack : e));
}

/************ ИСТОЧНИК (открытие по ID/URL/SELF с проверками) ************/
function getSourceSpreadsheet_() {
  const raw = String(SOURCE_SPREADSHEET_ID || '').trim();
  if (!raw) {
    throw new Error('SOURCE_SPREADSHEET_ID пуст. Укажите ID, полный URL или "SELF".');
  }
  if (raw.toUpperCase() === 'SELF') {
    return SpreadsheetApp.getActive();
  }

  let id = raw;
  if (/^https?:\/\//i.test(raw)) {
    const m = raw.match(/\/d\/([a-zA-Z0-9-_]+)/);
    if (!m) throw new Error('Не удалось извлечь ID из URL источника (ожидается .../d/<ID>/edit).');
    id = m[1];
  }

  try {
    const file = DriveApp.getFileById(id);
    const mime = file.getMimeType();
    const isSheet = mime === MimeType.GOOGLE_SHEETS || mime === 'application/vnd.google-apps.spreadsheet';
    if (!isSheet) throw new Error('Источник не является Google Таблицей. Сконвертируйте и используйте её ID.');
  } catch (e) {
    throw new Error('Нет доступа к источнику или неверный ID: ' + id + '. Детали: ' + e);
  }

  return SpreadsheetApp.openById(id);
}

/************ СЕРВИСНЫЕ УТИЛИТЫ ************/
function deleteAllTriggers() {
  ScriptApp.getProjectTriggers().forEach(t => ScriptApp.deleteTrigger(t));
  Logger.log('Все триггеры удалены.');
}

function debugSourceOpen_() {
  const ss = getSourceSpreadsheet_();
  const names = ss.getSheets().map(s => s.getName());
  Logger.log('Источник открыт. Листы: ' + names.join(', '));
  const srcSheet = ss.getSheetByName(SOURCE_SHEET_NAME);
  if (!srcSheet) throw new Error('В источнике не найден лист "' + SOURCE_SHEET_NAME + '".');
  Logger.log('Лист найден. Строк: ' + srcSheet.getLastRow() + ', кол.: ' + srcSheet.getLastColumn());
}

function debugQueueState_() {
  const ss = SpreadsheetApp.getActive();
  const q = ss.getSheetByName(QUEUE_SHEET_NAME);
  if (!q) {
    Logger.log('Лист очереди не найден: ' + QUEUE_SHEET_NAME);
    return;
  }
  const rows = q.getLastRow();
  const cols = q.getLastColumn();
  const next = getIntProp_(PROP_QUEUE_NEXT, 0);
  const last = getIntProp_(PROP_QUEUE_LAST, 0);
  Logger.log(`QUEUE SHEET: rows=${rows}, cols=${cols}, next=${next}, last=${last}`);
  if (rows >= 2) {
    const sample = q.getRange(2, 1, Math.min(3, rows - 1), Math.min(5, cols)).getValues();
    Logger.log('Первые строки очереди (частично): ' + JSON.stringify(sample));
  } else {
    Logger.log('Очередь пуста (только шапка или пусто).');
  }
}

function testDequeueOnce_() {
  dequeueOneHourly();
  Logger.log('Выполнена разовая выгрузка 1 «пачки» по Date-time.');
}

function fixQueuePointers_() {
  const ss = SpreadsheetApp.getActive();
  const q = ss.getSheetByName(QUEUE_SHEET_NAME);
  if (!q) throw new Error('Лист очереди не найден: ' + QUEUE_SHEET_NAME);
  const rows = q.getLastRow();
  if (rows <= 1) {
    setProp_(PROP_QUEUE_NEXT, '2');
    setProp_(PROP_QUEUE_LAST, '1');
    Logger.log('Очередь пуста. next=2, last=1');
  } else {
    setProp_(PROP_QUEUE_NEXT, '2');
    setProp_(PROP_QUEUE_LAST, String(rows));
    Logger.log(`Указатели сброшены. next=2, last=${rows}`);
  }
}

function resetTriggerTo1Minute_() {
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'dequeueOneHourly')
    .forEach(t => ScriptApp.deleteTrigger(t));

  ScriptApp.newTrigger('dequeueOneHourly')
    .timeBased()
    .everyMinutes(1)
    .create();

  Logger.log('Триггер «каждую минуту» установлен.');
}

function resetTriggerTo60Minute_() {
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'dequeueOneHourly')
    .forEach(t => ScriptApp.deleteTrigger(t));

  ScriptApp.newTrigger('dequeueOneHourly')
    .timeBased()
    .everyHours(1)
    .create();

  Logger.log('Триггер «каждый час» установлен.');
}

/************ TELEGRAM: отправка и авто-регистрация chat_id ************/
function sendTelegramMessage_(text) {
  if (!TELEGRAM_ENABLED) return;
  if (!TELEGRAM_BOT_TOKEN || TELEGRAM_BOT_TOKEN.indexOf(':') === -1) {
    Logger.log('TELEGRAM: токен не задан/неверен.');
    return;
  }

  // Текущие chat_id: сохранённые + из конфига.
  const storedIds = getStoredChatIds_();
  const manualIds = Array.isArray(TELEGRAM_CHAT_IDS) ? TELEGRAM_CHAT_IDS.map(String) : [];
  let chatIds = Array.from(new Set([...storedIds, ...manualIds]));

  // При необходимости — авто-подхват новых chat_id.
  if (TELEGRAM_AUTOREGISTER) {
    try {
      const refreshed = pollTelegramUpdatesAndRegister_();
      chatIds = Array.from(new Set([...chatIds, ...refreshed]));
    } catch (e) {
      Logger.log('TELEGRAM: ошибка авто-регистрации: ' + (e && e.stack ? e.stack : e));
    }
  }

  if (!chatIds.length) {
    Logger.log('TELEGRAM: нет chat_id. Добавьте бота в чат и отправьте любое сообщение — id подхватится автоматически.');
    return;
  }

  const url = 'https://api.telegram.org/bot' + TELEGRAM_BOT_TOKEN + '/sendMessage';

  chatIds.forEach(chatId => {
    try {
      const params = {
        method: 'post',
        payload: {
          chat_id: String(chatId),
          text: text,
          parse_mode: TELEGRAM_PARSE_MODE,
          disable_web_page_preview: true
        },
        muteHttpExceptions: true
      };
      const res = UrlFetchApp.fetch(url, params);
      const code = res.getResponseCode();
      if (code >= 300) {
        Logger.log('TELEGRAM: HTTP ' + code + ' → ' + res.getContentText());
      } else {
        Logger.log('TELEGRAM: отправлено в чат ' + chatId);
      }
    } catch (e) {
      Logger.log('TELEGRAM: исключение при отправке в ' + chatId + ': ' + (e && e.stack ? e.stack : e));
    }
  });
}

// Экранирование HTML для текста сообщений.
function escHtml_(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// Формат процента из доли (0.123 → 12.3%).
function formatPercent_(val) {
  const num = Number(val);
  if (!isFinite(num)) return String(val);
  return (num * 100).toFixed(1) + '%';
}

// Чтение/сохранение chat_id в Script Properties.
function getStoredChatIds_() {
  const sp = PropertiesService.getScriptProperties();
  const raw = sp.getProperty(PROP_TELEGRAM_CHAT_IDS_JSON);
  if (!raw) return [];
  try {
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? Array.from(new Set(arr.map(String))) : [];
  } catch (e) {
    Logger.log('TELEGRAM: повреждён список chat_id. Сбрасываю. ' + e);
    sp.deleteProperty(PROP_TELEGRAM_CHAT_IDS_JSON);
    return [];
  }
}

function saveStoredChatIds_(ids) {
  const uniq = Array.from(new Set((ids || []).map(String)));
  PropertiesService.getScriptProperties()
    .setProperty(PROP_TELEGRAM_CHAT_IDS_JSON, JSON.stringify(uniq));
  return uniq;
}

// Смещение getUpdates.
function getTelegramLastUpdateId_() {
  const v = PropertiesService.getScriptProperties().getProperty(PROP_TELEGRAM_LAST_UPDATE_ID);
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : null;
}
function setTelegramLastUpdateId_(id) {
  if (Number.isFinite(id)) {
    PropertiesService.getScriptProperties().setProperty(PROP_TELEGRAM_LAST_UPDATE_ID, String(id));
  }
}

/*
  pollTelegramUpdatesAndRegister_():
  - запрашивает новые апдейты Telegram (с учётом offset),
  - извлекает chat.id из разных типов событий,
  - сохраняет уникальные chat_id в Script Properties.
*/
function pollTelegramUpdatesAndRegister_() {
  if (!TELEGRAM_ENABLED) return getStoredChatIds_();
  if (!TELEGRAM_BOT_TOKEN || TELEGRAM_BOT_TOKEN.indexOf(':') === -1) return getStoredChatIds_();

  const url = 'https://api.telegram.org/bot' + TELEGRAM_BOT_TOKEN + '/getUpdates';
  const payload = { timeout: 0 };
  const lastId = getTelegramLastUpdateId_();
  if (Number.isFinite(lastId)) payload.offset = lastId + 1;

  const res = UrlFetchApp.fetch(url, { method: 'post', payload, muteHttpExceptions: true });
  if (res.getResponseCode() >= 300) {
    Logger.log('TELEGRAM getUpdates HTTP ' + res.getResponseCode() + ': ' + res.getContentText());
    return getStoredChatIds_();
  }

  const data = JSON.parse(res.getContentText());
  if (!data.ok) return getStoredChatIds_();

  const current = new Set(getStoredChatIds_().map(String));
  let maxUpdateId = lastId || null;

  const considerChat = (chat) => {
    if (!chat || chat.id == null) return;
    current.add(String(chat.id)); // Поддерживаются private, group, supergroup, channel.
  };

  (data.result || []).forEach(upd => {
    if (Number.isFinite(upd.update_id)) {
      if (maxUpdateId == null || upd.update_id > maxUpdateId) maxUpdateId = upd.update_id;
    }
    if (upd.message && upd.message.chat) considerChat(upd.message.chat);
    if (upd.edited_message && upd.edited_message.chat) considerChat(upd.edited_message.chat);
    if (upd.channel_post && upd.channel_post.chat) considerChat(upd.channel_post.chat);
    if (upd.edited_channel_post && upd.edited_channel_post.chat) considerChat(upd.edited_channel_post.chat);
    if (upd.my_chat_member && upd.my_chat_member.chat) considerChat(upd.my_chat_member.chat);
    if (upd.chat_member && upd.chat_member.chat) considerChat(upd.chat_member.chat);
  });

  if (maxUpdateId != null) setTelegramLastUpdateId_(maxUpdateId);
  const saved = saveStoredChatIds_(Array.from(current));
  Logger.log('TELEGRAM: зарегистрированы chat_id: ' + saved.join(', '));
  return saved;
}

// Служебные утилиты Telegram.
function telegramForcePoll_() {
  const ids = pollTelegramUpdatesAndRegister_();
  Logger.log('Force poll → chat_ids: ' + ids.join(', '));
}
function telegramShowStoredChats_() {
  Logger.log('Stored chat_ids: ' + getStoredChatIds_().join(', '));
}

/************ ВСПОМОГАТЕЛЬНОЕ: сравнение дат ************/
// Преобразует значение даты в ключ сравнения (timestamp). Если не распарсилось — использует строку.
function dateKey_(v) {
  if (Object.prototype.toString.call(v) === '[object Date]') {
    return String(v.getTime());
  }
  if (typeof v === 'number') return String(v);
  if (typeof v === 'string') {
    const parsed = parseDateTimeOrKeep_(v);
    if (Object.prototype.toString.call(parsed) === '[object Date]') {
      return String(parsed.getTime());
    }
    return v.trim();
  }
  return String(v);
}
