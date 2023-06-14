#define TINY_GSM_MODEM_SIM800

#include <WiFi.h>
#include <Preferences.h>
#include "time.h"
#include <TinyGsmClient.h>
#include <Wire.h>

#include <esp_task_wdt.h> //временно

/* for SD card*/
#include "FS.h"
#include "SD.h"
#include "SPI.h"

#include "secrets.h"

#include "languages.h"

#include <MQTTClient.h>
#include <ArduinoJson.h>

#include <SSLClient.h>
#include "AWS_Root_CA.h" // This file is created using AmazonRootCA1.pem from https://www.amazontrust.com/repository/AmazonRootCA1.pem

#include "RTClib.h"

#include <U8g2lib.h>

// U8G2_SSD1322_NHD_256X64_F_4W_SW_SPI u8g2(U8G2_R0, /* clock=*/ 26, /* data=*/ 25, /* cs=*/ 14, /* dc=*/ 15, /* reset=*/ 13);	// Enable U8G2_16BIT in u8g2.h

U8G2_SSD1322_NHD_256X64_F_4W_HW_SPI u8g2(U8G2_R0, /* cs=*/14, /* dc=*/15, /* reset=*/13); // Enable U8G2_16BIT in u8g2.h

RTC_PCF8523 rtc;

SPIClass SD_SPI(HSPI);

// #define SerialIO Serial
#define SerialSEC Serial1
#define SerialAT Serial2

#define SerialDebug Serial

TinyGsm modem(SerialAT);
TinyGsmClient client(modem);

WiFiClient wifi_client;

SSLClientParameters mTLS = SSLClientParameters::fromPEM(AWS_CERT_CRT, sizeof AWS_CERT_CRT, AWS_CERT_PRIVATE, sizeof AWS_CERT_PRIVATE);

MQTTClient wifi_mqtt_client = MQTTClient(2048);
MQTTClient gsm_mqtt_client = MQTTClient(2048);

Preferences preferences;

uint32_t chipId = 0;

int u = 0; // ????????????????????????????????????????

float coeff = 0.33;

uint8_t blackbox_files[1024]; // for SD read file, список файлов blackbox, индекс элемента - название файла, элемент 0 или 1 - наличие файла

long start_time = 0; // UNIX время включения устройства

/* переменные для совместного доступа из разных задач */
uint8_t global_internet = 0;          // есть ли интернет: 0 - не подключен, 1 - по GSM, 2 - по WiFi
uint8_t global_aws_internet = 2;      // подключен ли AWS: 0 - не подключен, 1 - по GSM, 2 - по WiFi
uint8_t global_cards_downloading = 0; // процесс обновления карточек с сервера, 0-не идет, 1-в процессе, 2-завершен успешно, 3-завершен с ошибкой
/* переменные для совместного доступа из разных задач */

struct card_struct // будет возвращать функция поиска карточки в файле
{
  bool is_present;
  uint8_t type_card;
  String user_driver;
  String car;
  int limit;
};

// конфигурационные параметры:
uint8_t config_GSM = 1;  // использовать gsm
uint8_t config_WiFi = 1; // использовать wifi
String config_GSM_APN = "internet";
// String config_WiFi_ssid = "INVTB";
String config_WiFi_ssid = "HUAWEI";
String config_WiFi_password = "12345678";
uint8_t language = 0;        // 0 - english, 1 - ukrainian, 2 - polish
long cards_updated_time = 0; // UNIX время включения устройства

/* тотал счетчики отпущенного топлива, сохраняются в preferences */
double counter = 0;

// Use WiFiClient class to create TCP connections
// WiFiClient client;

// функции
void save_config_params_to_flash();
void read_config_params_from_flash();
bool connectAWS(SSLClient &sequreclient, MQTTClient &mqttclient);
void WiFiEvent(WiFiEvent_t event);
uint8_t RTC_initialize();
bool SD_initialize();
bool time_from_ntp();
bool blackbox_write(String input);
String blackbox_read();
void blackbox_table_increment(uint16_t lenght);
card_struct find_in_cards(char input[]);
uint8_t sendfromblackbox_via_mqtt(MQTTClient &mqttclient);
void display(String text);
void display2lines(String uppertext, String bottomtext);

void wifi_messageHandler(String &topic, String &payload);
void gsm_messageHandler(String &topic, String &payload);

void TaskGetInternet(void *pvParameters);
void TaskWifiAWS(void *pvParameters);
void TaskGSMAWS(void *pvParameters);
void TaskSaveLog(void *pvParameters);
void TaskTimeFromNTP(void *pvParameters);
void TaskReadSerials(void *pvParameters);
void Taskfilling_routine(void *pvParameters);

// Define a task handle and initialize it to NULL
TaskHandle_t TaskGetInternet_handle = NULL;
TaskHandle_t TaskWifiAWS_handle = NULL;
TaskHandle_t TaskGSMAWS_handle = NULL;
TaskHandle_t TaskSaveLog_handle = NULL;
TaskHandle_t TaskTimeFromNTP_handle = NULL;
TaskHandle_t TaskReadSerials_handle = NULL;
TaskHandle_t Taskfilling_routine_handle = NULL;

static SemaphoreHandle_t mutex_GSM;
static SemaphoreHandle_t mutex_WiFi;
static SemaphoreHandle_t mutex_SD_card;
static SemaphoreHandle_t mutex_RTC;
static SemaphoreHandle_t mutex_global_variables;

/*очереди для работы с Serial'ами дополнительных плат*/
QueueHandle_t received_nfc_Queue;
QueueHandle_t received_button_Queue;
QueueHandle_t received_pulser_Queue;
/*очереди для работы с Serial'ами дополнительных плат*/

QueueHandle_t mqtt_send_Queue;
QueueHandle_t mqtt_response_received_Queue;

void save_config_params_to_flash()
{
  preferences.begin("gasstation", false);
  preferences.clear();

  preferences.putInt("gsm", config_GSM);
  preferences.putInt("wifi", config_WiFi);
  preferences.putString("apn", config_GSM_APN);
  preferences.putString("ssid", config_WiFi_ssid);
  preferences.putString("pass", config_WiFi_password);
  preferences.putLong64("cu_time", cards_updated_time);

  preferences.putDouble("cnt", counter);

  preferences.end();
  SerialDebug.println("Prefs saved OK");
}

void save_total_counter(uint8_t fuel_type)
{
  preferences.begin("gasstation", false);

  preferences.putDouble("cnt", counter);

  preferences.end();
  SerialDebug.println("Counter" + String(fuel_type) + " saved OK");
}

void read_config_params_from_flash()
{

  preferences.begin("gasstation", true);

  config_GSM = preferences.getInt("gsm", 1);
  config_WiFi = preferences.getInt("wifi", 0);
  config_GSM_APN = preferences.getString("apn", "internet");
  config_WiFi_ssid = preferences.getString("ssid", "HUAWEI");
  config_WiFi_password = preferences.getString("pass", "12345678");
  cards_updated_time = preferences.getLong64("cu_time", 0);

  counter = preferences.getDouble("cnt", 0);

  preferences.end();
}

bool connectAWS(SSLClient &sequreclient, MQTTClient &mqttclient)
{

  String wifi_or_gsm = "";

  if (mqttclient.connected())
    mqttclient.disconnect();

  sequreclient.setMutualAuthParams(mTLS);
  mqttclient.begin(AWS_IOT_ENDPOINT, 8883, sequreclient);

  // Create a message handler
  if (&mqttclient == &wifi_mqtt_client)
  {
    wifi_mqtt_client.onMessage(wifi_messageHandler);
    wifi_or_gsm = "Wifi";
  }
  else if (&mqttclient == &gsm_mqtt_client)
  {
    gsm_mqtt_client.onMessage(gsm_messageHandler);
    wifi_or_gsm = "GSM";
  }

  SerialDebug.print("Connecting to AWS IOT");

  for (uint8_t i = 0; i < 5; i++) // пробуем подключиться 5 раз
  {
    SerialDebug.println(wifi_or_gsm + " Connecting to AWS IOT try number " + String(i + 1));

    char chipid_str[10];
    itoa(chipId, chipid_str, 10);

    if (mqttclient.connect(chipid_str))
    {
      break;
    }
  }

  mqttclient.setKeepAlive(60);

  if (!mqttclient.connected())
  {
    SerialDebug.println(wifi_or_gsm + " AWS IoT Timeout!");
    return false;
  }

  // Subscribe to a topic
  mqttclient.subscribe("gasstation/card/" + String(chipId), 1);
  mqttclient.subscribe("gasstation/setting/" + String(chipId), 1);
  mqttclient.subscribe("gasstation/commands/" + String(chipId), 1);
  mqttclient.subscribe("gasstation/fuel/" + String(chipId), 1);

  SerialDebug.println(wifi_or_gsm + " AWS IoT Connected!");

  return true;
}

void wifi_messageHandler(String &topic, String &payload) // парсим полученное от сервера по mqtt
{
  DynamicJsonDocument message_json(2048);
  File file;
  SerialDebug.println(millis());
  SerialDebug.println("incoming: " + topic + " - " + payload);

  SerialDebug.println(millis());
  uint8_t cards_downloading4; // локальная копия глобальной переменной global_cards_downloading
  xSemaphoreTake(mutex_global_variables, portMAX_DELAY);
  cards_downloading4 = global_cards_downloading;
  xSemaphoreGive(mutex_global_variables);
  SerialDebug.println(millis());

  SerialDebug.println(millis());

  if (topic == "gasstation/card/" + String(chipId) && cards_downloading4 == 1)
  { // если пришло сообщение с карточками

    deserializeJson(message_json, payload);

    xSemaphoreTake(mutex_SD_card, portMAX_DELAY);

    if (message_json["skip"] == 0)
      file = SD.open("/cards/cards_tmp", FILE_WRITE);
    else
      file = SD.open("/cards/cards_tmp", FILE_APPEND);

    if (!file)
    {
      SerialDebug.println("Failed to create or append file cards_tmp");
      xSemaphoreGive(mutex_SD_card);
      cards_downloading4 = 3;
      xSemaphoreTake(mutex_global_variables, portMAX_DELAY);
      global_cards_downloading = cards_downloading4;
      xSemaphoreGive(mutex_global_variables);
      return;
    }

    // если в сообщение количество записей отличается от amount - ошибка
    // если amount < limit то больше не запрашиваем

    uint16_t limit = message_json["limit"];
    uint16_t skip = message_json["skip"];
    uint16_t amount = message_json["amount"];

    SerialDebug.print("aaaaa ");
    SerialDebug.print(limit);
    SerialDebug.print(skip);
    SerialDebug.println(amount);

    for (uint16_t i = 0; i < amount; i++)
    {
      if (message_json["data"][i])
      {
        // пишу в файл
        const char *data = message_json["data"][i];
        if (!file.println(data))
        {
          SerialDebug.println("ERROR Write failed");
          file.close();
          xSemaphoreGive(mutex_SD_card);
          cards_downloading4 = 3;
          xSemaphoreTake(mutex_global_variables, portMAX_DELAY);
          global_cards_downloading = cards_downloading4;
          xSemaphoreGive(mutex_global_variables);
          return;
        }
      }
      else
      {
        SerialDebug.println("ERROR Not valid cards string received, leaving...");
        file.close();
        cards_downloading4 = 3;
        xSemaphoreGive(mutex_SD_card);
        xSemaphoreTake(mutex_global_variables, portMAX_DELAY);
        global_cards_downloading = cards_downloading4;
        xSemaphoreGive(mutex_global_variables);
        return;
      }
    }

    file.close();

    if (amount == limit)
    {
      SerialDebug.println("requesting next");
      // запрашиваем следующую порцию карточек
      xSemaphoreGive(mutex_SD_card);
      if (!wifi_mqtt_client.publish("gasstation/card", "{\"deviceid\":\"" + String(chipId) + "\",\"limit\":20,\"skip\": " + String(skip + 20) + "}", 0, 1))
      {
        cards_downloading4 = 3; //
        SerialDebug.println("requesting next error");
      }
    }
    else if (amount < limit)
    {
      SerialDebug.println("download ok");
      // если amount < limit значит мы получили все карточки от сервера - заканчиваем обновление
      SD.remove("/cards/cards");
      if (SD.rename("/cards/cards_tmp", "/cards/cards"))
      {
        cards_downloading4 = 2;
        SerialDebug.println("File cards deleted, file cards_tmp renamed to cards");
      }
      else
      {
        cards_downloading4 = 3;
        SerialDebug.println("Rename failed");
      }
    }
    xSemaphoreGive(mutex_SD_card);
  }
  else if (topic == "gasstation/fuel/" + String(chipId))
  {
    char ght[256];
    payload.toCharArray(ght, 256);
    xQueueSend(mqtt_response_received_Queue, &ght, 0);
  }
  else if (topic == "gasstation/setting/" + String(chipId))
  {
    payload.replace("[{", "{");
    payload.replace("}]", "}");
    deserializeJson(message_json, payload);
    if (message_json["id_station"] == "16340452")
    {
      const char *wifi_ssid = message_json["wifi_ssid"];
      const char *wifi_pass = message_json["wifi_pass"];
      const char *gsm_apn = message_json["gsm_apn"];
      const char *mqtt_server = message_json["mqtt_server"];
      const char *mqtt_certificate = message_json["mqtt_certificate"];
      const char *mqtt_login = message_json["mqtt_login"];
      const char *mqtt_pass = message_json["mqtt_pass"];
      SerialDebug.print("Settings message received: ");
      SerialDebug.println(payload);
    }
  }
  else if (topic == "gasstation/service/" + String(chipId))
  {
    deserializeJson(message_json, payload);
    if (message_json["skip"] == 0)
    {
      ;
    }
  }
  else if (topic == "gasstation/commands/" + String(chipId))
  {
    deserializeJson(message_json, payload);
    if (message_json["skip"] == 0)
    {
      ;
    }
  }
  xSemaphoreTake(mutex_global_variables, portMAX_DELAY);
  global_cards_downloading = cards_downloading4;
  xSemaphoreGive(mutex_global_variables);
}

void gsm_messageHandler(String &topic, String &payload) // парсим полученное от сервера по mqtt
{
  DynamicJsonDocument message_json(2048);
  File file;
  SerialDebug.println(millis());
  SerialDebug.println("incoming: " + topic + " - " + payload);

  SerialDebug.println(millis());
  uint8_t cards_downloading4; // локальная копия глобальной переменной global_cards_downloading
  xSemaphoreTake(mutex_global_variables, portMAX_DELAY);
  cards_downloading4 = global_cards_downloading;
  xSemaphoreGive(mutex_global_variables);
  SerialDebug.println(millis());

  SerialDebug.println(millis());

  if (topic == "gasstation/card/" + String(chipId) && cards_downloading4 == 1)
  { // если пришло сообщение с карточками

    deserializeJson(message_json, payload);

    xSemaphoreTake(mutex_SD_card, portMAX_DELAY);

    if (message_json["skip"] == 0)
      file = SD.open("/cards/cards_tmp", FILE_WRITE);
    else
      file = SD.open("/cards/cards_tmp", FILE_APPEND);

    if (!file)
    {
      SerialDebug.println("Failed to create or append file cards_tmp");
      xSemaphoreGive(mutex_SD_card);
      cards_downloading4 = 3;
      xSemaphoreTake(mutex_global_variables, portMAX_DELAY);
      global_cards_downloading = cards_downloading4;
      xSemaphoreGive(mutex_global_variables);
      return;
    }

    // если в сообщение количество записей отличается от amount - ошибка
    // если amount < limit то больше не запрашиваем

    uint16_t limit = message_json["limit"];
    uint16_t skip = message_json["skip"];
    uint16_t amount = message_json["amount"];

    SerialDebug.print("aaaaa ");
    SerialDebug.print(limit);
    SerialDebug.print(skip);
    SerialDebug.println(amount);

    for (uint16_t i = 0; i < amount; i++)
    {
      if (message_json["data"][i])
      {
        // пишу в файл
        const char *data = message_json["data"][i];
        if (!file.println(data))
        {
          SerialDebug.println("ERROR Write failed");
          file.close();
          xSemaphoreGive(mutex_SD_card);
          cards_downloading4 = 3;
          xSemaphoreTake(mutex_global_variables, portMAX_DELAY);
          global_cards_downloading = cards_downloading4;
          xSemaphoreGive(mutex_global_variables);
          return;
        }
      }
      else
      {
        SerialDebug.println("ERROR Not valid cards string received, leaving...");
        file.close();
        cards_downloading4 = 3;
        xSemaphoreGive(mutex_SD_card);
        xSemaphoreTake(mutex_global_variables, portMAX_DELAY);
        global_cards_downloading = cards_downloading4;
        xSemaphoreGive(mutex_global_variables);
        return;
      }
    }

    file.close();

    if (amount == limit)
    {
      SerialDebug.println("requesting next");
      // запрашиваем следующую порцию карточек
      xSemaphoreGive(mutex_SD_card);
      if (!gsm_mqtt_client.publish("gasstation/card", "{\"deviceid\":\"" + String(chipId) + "\",\"limit\":20,\"skip\": " + String(skip + 20) + "}", 0, 1))
      {
        cards_downloading4 = 3; //
        SerialDebug.println("requesting next error");
      }
    }
    else if (amount < limit)
    {
      SerialDebug.println("download ok");
      // если amount < limit значит мы получили все карточки от сервера - заканчиваем обновление
      SD.remove("/cards/cards");
      if (SD.rename("/cards/cards_tmp", "/cards/cards"))
      {
        cards_downloading4 = 2;
        SerialDebug.println("File cards deleted, file cards_tmp renamed to cards");
      }
      else
      {
        cards_downloading4 = 3;
        SerialDebug.println("Rename failed");
      }
    }
    xSemaphoreGive(mutex_SD_card);
  }
  else if (topic == "gasstation/fuel/" + String(chipId))
  {
    char ght[256];
    payload.toCharArray(ght, 256);
    xQueueSend(mqtt_response_received_Queue, &ght, 0);
  }
  else if (topic == "gasstation/setting/" + String(chipId))
  {
    payload.replace("[{", "{");
    payload.replace("}]", "}");
    deserializeJson(message_json, payload);
    if (message_json["id_station"] == "16340452")
    {
      const char *wifi_ssid = message_json["wifi_ssid"];
      const char *wifi_pass = message_json["wifi_pass"];
      const char *gsm_apn = message_json["gsm_apn"];
      const char *mqtt_server = message_json["mqtt_server"];
      const char *mqtt_certificate = message_json["mqtt_certificate"];
      const char *mqtt_login = message_json["mqtt_login"];
      const char *mqtt_pass = message_json["mqtt_pass"];
      SerialDebug.print("Settings message received: ");
      SerialDebug.println(payload);
    }
  }
  else if (topic == "gasstation/service/" + String(chipId))
  {
    deserializeJson(message_json, payload);
    if (message_json["skip"] == 0)
    {
      ;
    }
  }
  else if (topic == "gasstation/commands/" + String(chipId))
  {
    deserializeJson(message_json, payload);
    if (message_json["skip"] == 0)
    {
      ;
    }
  }
  xSemaphoreTake(mutex_global_variables, portMAX_DELAY);
  global_cards_downloading = cards_downloading4;
  xSemaphoreGive(mutex_global_variables);
}

void WiFiEvent(WiFiEvent_t event)
{
  SerialDebug.print("[WiFi-event]: ");

  switch (event)
  {
  case ARDUINO_EVENT_WIFI_READY:
    SerialDebug.println("WiFi interface ready");
    break;
  case ARDUINO_EVENT_WIFI_SCAN_DONE:
    SerialDebug.println("Completed scan for access points");
    break;
  case ARDUINO_EVENT_WIFI_STA_START:
    SerialDebug.println("WiFi client started");
    break;
  case ARDUINO_EVENT_WIFI_STA_STOP:
    SerialDebug.println("WiFi clients stopped");
    break;
  case ARDUINO_EVENT_WIFI_STA_CONNECTED:
    SerialDebug.println("Connected to access point");
    break;
  case ARDUINO_EVENT_WIFI_STA_DISCONNECTED:
    xSemaphoreTake(mutex_global_variables, portMAX_DELAY);
    if (global_internet == 2)
    {
      SerialDebug.println("Disconnected from WiFi access point");
      global_internet = 1;
    }
    xSemaphoreGive(mutex_global_variables);
    break;
  case ARDUINO_EVENT_WIFI_STA_AUTHMODE_CHANGE:
    SerialDebug.println("Authentication mode of access point has changed");
    break;
  case ARDUINO_EVENT_WIFI_STA_GOT_IP:
    SerialDebug.print("Obtained IP address: ");
    SerialDebug.println(WiFi.localIP());
    break;
  case ARDUINO_EVENT_WIFI_STA_LOST_IP:
    SerialDebug.println("Lost IP address and IP address is reset to 0");
    break;
  case ARDUINO_EVENT_WPS_ER_SUCCESS:
    SerialDebug.println("WiFi Protected Setup (WPS): succeeded in enrollee mode");
    break;
  case ARDUINO_EVENT_WPS_ER_FAILED:
    SerialDebug.println("WiFi Protected Setup (WPS): failed in enrollee mode");
    break;
  case ARDUINO_EVENT_WPS_ER_TIMEOUT:
    SerialDebug.println("WiFi Protected Setup (WPS): timeout in enrollee mode");
    break;
  case ARDUINO_EVENT_WPS_ER_PIN:
    SerialDebug.println("WiFi Protected Setup (WPS): pin code in enrollee mode");
    break;
  case ARDUINO_EVENT_WIFI_AP_START:
    SerialDebug.println("WiFi access point started");
    break;
  case ARDUINO_EVENT_WIFI_AP_STOP:
    SerialDebug.println("WiFi access point  stopped");
    break;
  case ARDUINO_EVENT_WIFI_AP_STACONNECTED:
    SerialDebug.println("Client connected");
    break;
  case ARDUINO_EVENT_WIFI_AP_STADISCONNECTED:
    SerialDebug.println("Client disconnected");
    break;
  case ARDUINO_EVENT_WIFI_AP_STAIPASSIGNED:
    SerialDebug.println("Assigned IP address to client");
    break;
  case ARDUINO_EVENT_WIFI_AP_PROBEREQRECVED:
    SerialDebug.println("Received probe request");
    break;
  case ARDUINO_EVENT_WIFI_AP_GOT_IP6:
    SerialDebug.println("AP IPv6 is preferred");
    break;
  case ARDUINO_EVENT_WIFI_STA_GOT_IP6:
    SerialDebug.println("STA IPv6 is preferred");
    break;
  case ARDUINO_EVENT_ETH_GOT_IP6:
    SerialDebug.println("Ethernet IPv6 is preferred");
    break;
  case ARDUINO_EVENT_ETH_START:
    SerialDebug.println("Ethernet started");
    break;
  case ARDUINO_EVENT_ETH_STOP:
    SerialDebug.println("Ethernet stopped");
    break;
  case ARDUINO_EVENT_ETH_CONNECTED:
    SerialDebug.println("Ethernet connected");
    break;
  case ARDUINO_EVENT_ETH_DISCONNECTED:
    SerialDebug.println("Ethernet disconnected");
    break;
  case ARDUINO_EVENT_ETH_GOT_IP:
    SerialDebug.println("Obtained IP address");
    break;
  default:
    break;
  }
}

uint8_t RTC_initialize()
{

  SerialDebug.println("Initializing RTC...");

  Wire1.setPins(21, 22);

  if (!rtc.begin(&Wire1))
  {
    SerialDebug.println("Couldn't find RTC");
    return 0;
  }

  if (!rtc.initialized() || rtc.lostPower())
  {
    SerialDebug.println("RTC is NOT initialized, let's set the time!");
    // When time needs to be set on a new device, or after a power loss, the
    // following line sets the RTC to the date & time this sketch was compiled
    rtc.adjust(DateTime(2000, 1, 1, 0, 0, 0));
    // This line sets the RTC with an explicit date & time, for example to set
    // January 21, 2014 at 3am you would call:
    // rtc.adjust(DateTime(2014, 1, 21, 3, 0, 0));
    //
    // Note: allow 2 seconds after inserting battery or applying external power
    // without battery before calling adjust(). This gives the PCF8523's
    // crystal oscillator time to stabilize. If you call adjust() very quickly
    // after the RTC is powered, lostPower() may still return true.
  }

  // When the RTC was stopped and stays connected to the battery, it has
  // to be restarted by clearing the STOP bit. Let's do this to ensure
  // the RTC is running.
  rtc.start();

  // The PCF8523 can be calibrated for:
  //        - Aging adjustment
  //        - Temperature compensation
  //        - Accuracy tuning
  // The offset mode to use, once every two hours or once every minute.
  // The offset Offset value from -64 to +63. See the Application Note for calculation of offset values.
  // https://www.nxp.com/docs/en/application-note/AN11247.pdf
  // The deviation in parts per million can be calculated over a period of observation. Both the drift (which can be negative)
  // and the observation period must be in seconds. For accuracy the variation should be observed over about 1 week.
  // Note: any previous calibration should cancelled prior to any new observation period.
  // Example - RTC gaining 43 seconds in 1 week
  float drift = 43;                                     // seconds plus or minus over oservation period - set to 0 to cancel previous calibration.
  float period_sec = (7 * 86400);                       // total obsevation period in seconds (86400 = seconds in 1 day:  7 days = (7 * 86400) seconds )
  float deviation_ppm = (drift / period_sec * 1000000); //  deviation in parts per million (μs)
  float drift_unit = 4.34;                              // use with offset mode PCF8523_TwoHours
  // float drift_unit = 4.069; //For corrections every min the drift_unit is 4.069 ppm (use with offset mode PCF8523_OneMinute)
  int offset = round(deviation_ppm / drift_unit);
  // rtc.calibrate(PCF8523_TwoHours, offset); // Un-comment to perform calibration once drift (seconds) and observation period (seconds) are correct
  // rtc.calibrate(PCF8523_TwoHours, 0); // Un-comment to cancel previous calibration

  SerialDebug.print("RTC offset is ");
  SerialDebug.println(offset); // Print to control offset

  SerialDebug.printf("RTC time: %d-%d-%d, %02d:%02d:%02d\r\n", rtc.now().year(), rtc.now().month(), rtc.now().day(), rtc.now().hour(), rtc.now().minute(), rtc.now().second());

  SerialDebug.printf("RTC Unix time = %d\r\n", rtc.now().unixtime());

  return 1;
}

bool SD_initialize()
{
  File root;
  File file;

  // очищаю список blackbox файлов
  for (uint16_t i = 0; i < 1024; i++)
  {
    blackbox_files[i] = 0;
  }

  pinMode(5, OUTPUT);
  digitalWrite(5, HIGH); // power off to reset
  delay(1000);
  digitalWrite(5, LOW); // power on

  SD_SPI.begin(18, 19, 23, 33);

  if (!SD.begin(33, SD_SPI))
  {
    SerialDebug.println("SD Card Mount Failed");
    return false;
  }

  switch (SD.cardType())
  {
  case CARD_NONE:
    SerialDebug.println("No SD card attached");
    return false;
    break;

  case CARD_MMC:
    SerialDebug.println("MMC card detected");
    break;

  case CARD_SD:
    SerialDebug.println("SDSC card detected");
    break;

  case CARD_SDHC:
    SerialDebug.println("SDHC card detected");
    break;

  default:
    SerialDebug.println("UNKNOWN card detected");
    break;
  }

  uint64_t cardSize = SD.cardSize() / (1024 * 1024);
  SerialDebug.printf("SD Card Size: %lluMB\n", cardSize);
  SerialDebug.printf("Total space: %lluMB\n", SD.totalBytes() / (1024 * 1024));
  SerialDebug.printf("Used space: %lluMB\n", SD.usedBytes() / (1024 * 1024));

  file = SD.open("/");
  if (file)
    SerialDebug.println("ROOT dir open OK");
  else
  {
    SerialDebug.println("ROOT dir open ERROR");
    return false;
  }
  file.close();

  file = SD.open("/blackbox");
  if (file && file.isDirectory())
    SerialDebug.println("blackbox directory exists");
  else
  {
    SerialDebug.print("Creating blackbox dir ...");
    if (SD.mkdir("/blackbox"))
      SerialDebug.println("blackbox dir created");
    else
    {
      SerialDebug.println("blackbox mkdir failed");
      file.close();
      return false;
    }
  }
  file.close();

  file = SD.open("/cards");
  if (file && file.isDirectory())
    SerialDebug.println("cards directory exists");
  else
  {
    SerialDebug.print("Creating cards dir ...");
    if (SD.mkdir("/cards"))
      SerialDebug.println("cards dir created");
    else
    {
      SerialDebug.println("cards mkdir failed");
      file.close();
      return false;
    }
  }
  file.close();

  root = SD.open("/blackbox");

  if (!root)
  {
    SerialDebug.println("Failed to open blackbox dir");
    return false;
  }

  // ищю все файлы и создаю их список
  String filename = "";
  file = root.openNextFile();
  while (file)
  {
    filename = file.name();
    blackbox_files[filename.toInt()] = 1;
    if (filename.toInt() == 0) // удаляю файлы с нечисловыми названиями
      SD.remove("/blackbox/" + filename);
    file = root.openNextFile();
  }

  // печатаю список найденных файлов
  SerialDebug.print("Blackbox files list: ");
  for (uint16_t i = 0; i < 1024; i++)
  {
    if (blackbox_files[i])
      SerialDebug.printf("%d ", i);
  }
  SerialDebug.println("");

  /*печатаю содержимое файлов cards*/
  file = SD.open("/cards/cards"); // вівожу содержимое файла cards
  if (!file)
  {
    SerialDebug.println("Failed to open file cards for reading");
  }

  SerialDebug.print("Read from cards file: ");
  while (file.available() > 0)
  {
    SerialDebug.write(file.read());
  }
  file.close();
  /*печатаю содержимое файлов cards*/

  /* смотрю есть ли несохраненніе заправки */
  if (SD.exists("/unsaved_data") && SD.exists("/unsaved_quant"))
  {
    SerialDebug.println("Unsaved filling found!!!!");
    DynamicJsonDocument file_json(512);
    char jsonBuffer[512];

    file = SD.open("/unsaved_data");
    if (!file)
      return true;

    deserializeJson(file_json, file.readString());
    file.close();

    file = SD.open("/unsaved_quant"); // вівожу содержимое файла unsaved_quant
    if (!file)
      return true;

    //   SerialDebug.print("Read from unsaved_quant file: ");
    float float_from_file = 0;
    float float_from_file_max = 0;
    while (file.available() > 0) // нахожу последнее значение количества
    {
      float_from_file = file.parseFloat();
      if (float_from_file > float_from_file_max)
      {
        float_from_file_max = float_from_file;
      }
    }
    //    Serial.println(float_from_file_max);
    file.close();

    counter = counter + float_from_file_max;

    file_json["date_end"] = rtc.now().unixtime();
    file_json["amount"] = String(float_from_file_max, 2);
    file_json["counter"] = String(counter, 2);

    serializeJson(file_json, jsonBuffer);
    save_config_params_to_flash();
    blackbox_write(jsonBuffer);

    SD.remove("/unsaved_data");
    SD.remove("/unsaved_quant");

    SerialDebug.print("Unsaved filling saved and deleted successfully: ");
    Serial.println(jsonBuffer);
  }
  else
    SerialDebug.println("There is no unsaved_data file - all fillings were saved.");
  /* смотрю есть ли несохраненніе заправки */

  return true;
}

bool time_from_ntp(String ntp_server_address, uint8_t internet)
{
  int year, mon, day, hour, min, sec;

  if (internet == 1) // GSM
  {
    if (modem.NTPServerSync(ntp_server_address, 0))
    {
      if (modem.getNetworkTime(&year, &mon, &day, &hour, &min, &sec, 0))
      {
        year = year - 2000;
      }
      else
      {
        SerialDebug.println("getNetworkTime error");
        return false;
      }
    }
    else
    {
      SerialDebug.println("NTPServerSync error");
      return false;
    }
    SerialDebug.printf("GSM network time from NTP: %d-%d-%d   %d-%d-%d\r\n", year, mon, day, hour, min, sec);
  }
  else if (internet == 2) // WiFi
  {
    configTime(0, 0, ntp_server_address.c_str());

    struct tm timeinfo;
    if (!getLocalTime(&timeinfo))
    {
      SerialDebug.println("Failed to obtain time from NTP, RTC not updated");
      return false;
    }

    year = timeinfo.tm_year - 100;
    mon = timeinfo.tm_mon + 1;
    day = timeinfo.tm_mday;
    hour = timeinfo.tm_hour;
    min = timeinfo.tm_min;
    sec = timeinfo.tm_sec;
  }

  SerialDebug.printf("RTC time: %d-%d-%d, %02d:%02d:%02d    ", rtc.now().year(), rtc.now().month(), rtc.now().day(), rtc.now().hour(), rtc.now().minute(), rtc.now().second());

  if (year < 22)
    return false;

  xSemaphoreTake(mutex_RTC, portMAX_DELAY);
  rtc.adjust(DateTime(year + 2000, mon, day, hour, min, sec));
  xSemaphoreGive(mutex_RTC);

  SerialDebug.printf("updated from NTP: %d-%02d-%02d, %02d:%02d:%02d\r\n", year, mon, day, hour, min, sec);
  return true;
}

bool sim800_connect()
{

  SerialAT.updateBaudRate(115200);

  pinMode(32, OUTPUT);
  digitalWrite(32, LOW); // turn on SIM800
  delay(1300);
  digitalWrite(32, HIGH);

  if (modem.begin())
  {
    SerialDebug.println("modem begin OK");
  }
  else
  {
    SerialDebug.println("modem begin ERROR");

    return false;
  }

  if (modem.factoryDefault())
  {
    SerialDebug.println("modem factoryDefault OK");
  }
  else
  {
    SerialDebug.println("modem factoryDefault ERROR");
    return false;
  }

  SerialDebug.print(F("Initializing modem..."));

  if (modem.restart())
  {
    SerialDebug.println("OK");
  }
  else
  {
    SerialDebug.println("ERROR");
    return false;
  }

  modem.sendAT("+IPR=230400");
  if (modem.waitResponse())
    SerialDebug.println("SIM800 baud set to 230400 OK");
  else
  {
    SerialDebug.println("AT+IPR=230400 ERROR");
    return false;
  }

  SerialAT.updateBaudRate(230400);

  delay(1000);

  String modemInfo = modem.getModemInfo();
  SerialDebug.print(F("Modem: "));
  SerialDebug.println(modemInfo);

  SerialDebug.print(F("Waiting for network..."));
  if (!modem.waitForNetwork())
  {
    SerialDebug.println(" fail");
    delay(10000);
    return false;
  }
  SerialDebug.println(" OK");

  SerialDebug.print(F("Connecting to "));
  SerialDebug.print(config_GSM_APN);
  if (!modem.gprsConnect(config_GSM_APN.c_str(), "", ""))
  {
    SerialDebug.println(" fail");
    delay(10000);
    return false;
  }
  SerialDebug.println(" OK");
  SerialDebug.print("GSM IP address is: ");
  SerialDebug.println(modem.getLocalIP());

  return true;
}

bool WiFi_connect()
{
  SerialDebug.printf("\r\nWiFi Connecting to %s\r\n", config_WiFi_ssid);

  WiFi.disconnect();

  WiFi.begin(config_WiFi_ssid.c_str(), config_WiFi_password.c_str());
  // WiFi.begin("HUAWEI", "12345678");

  // Wait for connection
  long time_wifi = millis();
  while (WiFi.status() != WL_CONNECTED)
  {
    if (millis() - time_wifi > 5000)
    {
      SerialDebug.printf("\r\nWiFi Connect to %s error\r\n", config_WiFi_ssid);
      WiFi.disconnect();
      return false;
    }
    delay(500);
    SerialDebug.print(".");
  }

  SerialDebug.println("");
  SerialDebug.println("WiFi connected");
  //  SerialDebug.println("IP address: ");
  // SerialDebug.println(WiFi.localIP());
  return true;
}

void setup()
{

 esp_task_wdt_init(10, true);

  for (int i = 0; i < 17; i = i + 8)
  {
    chipId |= ((ESP.getEfuseMac() >> (40 - i)) & 0xff) << i;
  }

  mutex_SD_card = xSemaphoreCreateMutex();
  mutex_GSM = xSemaphoreCreateMutex();
  mutex_WiFi = xSemaphoreCreateMutex();
  mutex_RTC = xSemaphoreCreateMutex();
  mutex_global_variables = xSemaphoreCreateMutex();
  xSemaphoreGive(mutex_SD_card);
  xSemaphoreGive(mutex_GSM);
  xSemaphoreGive(mutex_WiFi);
  xSemaphoreGive(mutex_RTC);
  xSemaphoreGive(mutex_global_variables);

  received_nfc_Queue = xQueueCreate(1, sizeof(char[32]));
  received_button_Queue = xQueueCreate(5, sizeof(int));
  received_pulser_Queue = xQueueCreate(100, sizeof(int8_t));

  mqtt_send_Queue = xQueueCreate(1, sizeof(String));
  mqtt_response_received_Queue = xQueueCreate(1, sizeof(char[256]));

  SPI.begin(26, -1, 25, 14);

  u8g2.begin();
  u8g2.setFlipMode(1);
  u8g2.enableUTF8Print();

  // initializing UART for debug (softwareSerial)
  SerialDebug.begin(115200);

  // initializing UART for IO board
  // SerialIO.begin(9600, SERIAL_8N1, 25, 26);
  //  pinMode(25, INPUT_PULLUP); // SerialIO Rx pin pullup

  // initializing SIM800 UART
  int bu = SerialAT.setRxBufferSize(2048);
  SerialAT.begin(115200);
  SerialDebug.printf("SerialAT buffer set to %d\n", bu);

  // initializing SerialSEC UART
  SerialSEC.begin(115200, SERIAL_8N1, 27, 4);
  pinMode(27, INPUT_PULLUP); // SerialSEC Rx pin pullup

  delay(1000);

  SerialDebug.print("arduino running on core ");
  SerialDebug.println(ARDUINO_RUNNING_CORE);

  SerialDebug.print("Setup: priority = ");
  SerialDebug.println(uxTaskPriorityGet(NULL));

  if (!RTC_initialize())
    SerialDebug.println("RTC initialization error");
  else
  {
    SerialDebug.println("RTC initialization OK");
    start_time = rtc.now().unixtime();
  }

  SD_initialize();

  /*BLACKBOX READ-WRITE TEST*/ /*
   for (int i = 0; i < 100; i++)
   {
     blackbox_write("{VOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAVOVAV" + String(i) + "}");
     delay(5);
   }

   String read1;
   int pos_prev;
   int i = 0;
   for (i = 0; i < 60000; i++)
   {
     // SerialDebug.printf("t%d\r\n", millis());
     read1 = blackbox_read();
     //    SerialDebug.printf("T%d\r\n", millis());
     if (read1 != "")
       blackbox_table_increment(read1.length());
     else
       break;
     //    SerialDebug.printf("tT%d\r\n", millis());

     //   SerialDebug.println(read1);

     delay(5);
   }

   SerialDebug.printf("read %d items\r\n", i);       */
                               /*BLACKBOX READ-WRITE TEST*/

  xTaskCreatePinnedToCore(
      TaskGetInternet,         // Функция, содержащая код задачи
      "GetInternet",           // Название задачи
      10000,                   // Размер стека в словах
      NULL,                    // Параметр создаваемой задачи
      0,                       // Приоритет задачи
      &TaskGetInternet_handle, // Идентификатор
      0);                      // Ядро, на котором будет выполняться задача задачи

  xTaskCreatePinnedToCore(
      TaskWifiAWS,         // Функция, содержащая код задачи
      "WifiAWS",           // Название задачи
      40000,               // Размер стека в словах
      NULL,                // Параметр создаваемой задачи
      3,                   // Приоритет задачи
      &TaskWifiAWS_handle, // Идентификатор
      0);                  // Ядро, на котором будет выполняться задача задачи

  xTaskCreatePinnedToCore(
      TaskGSMAWS,         // Функция, содержащая код задачи
      "GSMAWS",           // Название задачи
      40000,              // Размер стека в словах
      NULL,               // Параметр создаваемой задачи
      3,                  // Приоритет задачи
      &TaskGSMAWS_handle, // Идентификатор
      0);                 // Ядро, на котором будет выполняться задача задачи

  xTaskCreatePinnedToCore(
      TaskTimeFromNTP,         // Функция, содержащая код задачи
      "TimeFromNTP",           // Название задачи
      10000,                   // Размер стека в словах
      NULL,                    // Параметр создаваемой задачи
      1,                       // Приоритет задачи
      &TaskTimeFromNTP_handle, // Идентификатор
      0);                      // Ядро, на котором будет выполняться задача задачи

  xTaskCreatePinnedToCore(
      TaskReadSerials,         // Функция, содержащая код задачи
      "ReadSerials",           // Название задачи
      10000,                   // Размер стека в словах
      NULL,                    // Параметр создаваемой задачи
      3,                       // Приоритет задачи
      &TaskReadSerials_handle, // Идентификатор
      1);                      // Ядро, на котором будет выполняться задача задачи

  xTaskCreatePinnedToCore(
      TaskSaveLog,         // Функция, содержащая код задачи
      "SaveLog",           // Название задачи
      10000,               // Размер стека в словах
      NULL,                // Параметр создаваемой задачи
      1,                   // Приоритет задачи
      &TaskSaveLog_handle, // Идентификатор
      1);                  // Ядро, на котором будет выполняться задача задачи

  xTaskCreatePinnedToCore(
      Taskfilling_routine, // Функция, содержащая код задачи
      "filling_routine",   // Название задачи
      20000,               // Размер стека в словах
      NULL,                // Параметр создаваемой задачи
      2,                   // Приоритет задачи
      NULL,                // Идентификатор
      1);                  // Ядро, на котором будет выполняться задача задачи
}

void loop()
{
  /*
    SerialDebug.println("variables: ");
    SerialDebug.println(global_aws_internet);
    SerialDebug.println(global_cards_downloading);
    SerialDebug.println(global_internet);
    // SerialDebug.println(blackbox_read());

    delay(2000);
  */
  // SerialDebug.print("Loop: priority = ");
  // SerialDebug.println(uxTaskPriorityGet(NULL));

  vTaskDelete(NULL);
  //  mqtt_client.loop();
  // delay(10);
}

uint8_t sendfromblackbox_via_mqtt(MQTTClient &mqttclient) // 0 - sending error, 1 - sending ok, 2 - empty blackbox
{

  String fromblackbox = blackbox_read();

  char mqtt_response[256];

  DynamicJsonDocument fuel_json(1024);
  DynamicJsonDocument answer_json(512);

  if (fromblackbox == "")
    return 2;

  SerialDebug.print("Blackbox message: ");
  SerialDebug.println(fromblackbox);
  fromblackbox += "\r\n";

  if (fromblackbox.indexOf("fuel") > 0)
  {

    // Serial.println(fromblackbox.length());
    deserializeJson(fuel_json, fromblackbox);

    if (mqttclient.publish("gasstation/fuel", fromblackbox, 0, 1))
    {
      long time_request_sent = millis();
      while (millis() - time_request_sent < 15000)
      {
        mqttclient.loop();
        if (xQueueReceive(mqtt_response_received_Queue, &mqtt_response, 20) == pdTRUE)
        {
          Serial.println(mqtt_response);
          deserializeJson(answer_json, mqtt_response);

          Serial.println(mqtt_response);

          if (answer_json["date_start"] == fuel_json["date_start"] && answer_json["date_end"] == fuel_json["date_end"] && (answer_json["result"] == "ok" || answer_json["result"] == "OK"))
          {
            blackbox_table_increment(fromblackbox.length());
            return 1;
          }
          else
            return 0;
        }
      }
      return 0;
    }
    else
      return 0;
  }
  else if (fromblackbox.indexOf("log") > 0)
  {
    if (mqttclient.publish("gasstation/service", fromblackbox, 0, 1))
    {
      blackbox_table_increment(fromblackbox.length());
      return 1;
    }
    else
    {
      return 0;
    }
  }
  else
  {
    blackbox_table_increment(fromblackbox.length());
    SerialDebug.println("invalid message from blackbox deleted");
    return 0;
  }
}

bool blackbox_write(String input)
{
  File file;
  String filename = "";
  bool result = false;

  for (uint16_t i = 1023; i > 0; i--) // находим последний файл
  {
    if (blackbox_files[i])
    {
      filename = String(i);
      break;
    }
  }

  xSemaphoreTake(mutex_SD_card, portMAX_DELAY);

  if (filename == "") // если нет ни одного файла - создаем файл 1
  {
    file = SD.open("/blackbox/1", FILE_WRITE);
    if (!file)
    {
      SerialDebug.println("ERROR blackbox_write - fail to create file 1");
      xSemaphoreGive(mutex_SD_card);
      return false;
    }
    if (!file.print(input))
    {
      SerialDebug.println("ERROR blackbox_write - write failed");
      result = false;
    }
    else
      result = true;
    blackbox_files[1] = 1;
    SerialDebug.println("blackbox_write - file created 1");
  }
  else // если файл есть - открываем его
  {
    file = SD.open("/blackbox/" + filename, FILE_APPEND);
    if (!file)
    {
      xSemaphoreGive(mutex_SD_card);
      SerialDebug.println("blackbox_write - fail to open expected file");
      return false;
    }
    else if (file.size() < 10000)
    {
      if (file.print(input))
      {
        result = true;
      }
      else
      {
        SerialDebug.println("ERROR blackbox_write - write failed");
        result = false;
      }
    }
    else
    {
      file.close();
      filename = String(filename.toInt() + 1);
      file = SD.open("/blackbox/" + filename, FILE_WRITE);
      if (!file)
      {
        xSemaphoreGive(mutex_SD_card);
        SerialDebug.println("ERROR blackbox_write - fail to create next file");
        return false;
      }

      SerialDebug.print("file created ");
      SerialDebug.println(filename);
      blackbox_files[filename.toInt()] = 1;
      if (file.print(input))
      {
        result = true;
      }
      else
      {
        SerialDebug.println("ERROR blackbox_write - fail to write to file");
        result = false;
      }
    }
  }
  file.close();

  xSemaphoreGive(mutex_SD_card);

  return result;
}

String blackbox_read()
{

  File file;
  String result = "";
  int position = 0;
  String filename;

  xSemaphoreTake(mutex_SD_card, portMAX_DELAY);

  for (uint8_t i = 0; i < 2; i++)
  {
    file = SD.open("/blackbox_table", FILE_READ);
    if (!file)
      position = 0;
    else
    {
      position = file.parseInt();
      file.close();
    }

    filename = "";
    for (uint16_t i = 1; i < 1024; i++) // находим первый файл
    {
      if (blackbox_files[i])
      {
        filename = String(i);
        break;
      }
    }
    if (filename == "")
    {
      SerialDebug.println("blackbox is empty");
      xSemaphoreGive(mutex_SD_card);
      return "";
    }

    file = SD.open("/blackbox/" + filename, FILE_READ);
    if (!file)
    {
      SerialDebug.println("ERROR blackbox_read - cant open expected file for read");
      xSemaphoreGive(mutex_SD_card);
      return "";
    }

    file.seek(position);
    //     Serial.println(position);

    //     Serial.println(file.available());

    while (file.available() > 0 && file.peek() != '{') // пропускаем невалидные символы, ищем начало сообщения
    {
      file.read();
    }

    if (file.available() > 0)
    { // читаю сообщение
      result = file.readStringUntil('}');
      result += '}';
      file.close();
    }
    else
    { // если файл закончился - удаляю его и файл таблицы
      SerialDebug.printf("file %d deleted\r\n", filename.toInt());
      file.close();
      SD.remove("/blackbox_table");
      SD.remove("/blackbox/" + filename);
      file = SD.open("/blackbox_table", FILE_WRITE);
      file.print(0);
      file.close();
      blackbox_files[filename.toInt()] = 0;
      continue;
    }

    file.close();
    break;
  }
  xSemaphoreGive(mutex_SD_card);

  return result;
}

void blackbox_table_increment(uint16_t lenght)
{
  uint32_t prev_value;

  xSemaphoreTake(mutex_SD_card, portMAX_DELAY);

  File file = SD.open("/blackbox_table", FILE_READ);
  if (!file)
    prev_value = 0;
  else
  {
    prev_value = file.parseInt();
    file.close();
  }
  file = SD.open("/blackbox_table", FILE_WRITE);
  if (!file)
    SerialDebug.println("ERROR blackbox_table_increment could not create file");
  if (!file.print(String(prev_value + lenght)))
    SerialDebug.println("ERROR blackbox_table_increment could not print to file");
  file.close();

  xSemaphoreGive(mutex_SD_card);
}

card_struct find_in_cards(char input[])
{
  card_struct result;

  File file;

  xSemaphoreTake(mutex_SD_card, portMAX_DELAY);

  file = SD.open("/cards/cards", FILE_READ);

  if (!file)
  {
    SerialDebug.println("ERROR find_in_cards file cards open error");
    result.is_present = false;
  }
  else
  {
    result.is_present = file.find(input);
    if (result.is_present)
    {
      if (file.read() == ';')
      {
        result.type_card = file.readStringUntil(';').toInt();
        result.user_driver = file.readStringUntil(';'); // skip this item
        result.car = file.readStringUntil(';');         // skip this item
        result.limit = file.readStringUntil(';').toInt();
        result.user_driver = file.readStringUntil(';');
        result.car = file.readStringUntil('\r');
      }
      else
      {
        result.is_present = false;
        SerialDebug.println("ERROR find_in_cards: after card number must be ; symbol");
      }
    }
    file.close();
  }
  xSemaphoreGive(mutex_SD_card);

  return result;
}

void send_to_OLED(String text)
{

  int16_t x = 0;
  int16_t y = 0;
  int16_t numLines = 0;

  const uint16_t SCREEN_WIDTH = 256;
  const uint8_t SCREEN_HEIGHT = 64;

  uint8_t MAX_LINES = 0;

  u8g2.clearBuffer(); // clear the internal memory

  switch (language) // choose a suitable font
  {
  case 0:
    u8g2.setFont(u8g2_font_unifont_t_cyrillic);
    break;

  case 1:
    u8g2.setFont(u8g2_font_unifont_t_cyrillic);
    break;

  case 2:
    u8g2.setFont(u8g2_font_unifont_t_polish);
    break;

  default:
    break;
  }

  y = u8g2.getMaxCharHeight() - 2;
  MAX_LINES = (SCREEN_HEIGHT / (u8g2.getMaxCharHeight()));

  // Разбиваем текст на отдельные слова
  String word;
  for (uint16_t i = 0; i < text.length(); i++)
  {
    char c = text.charAt(i);
    if (c == ' ' || c == '\n')
    {
      if (word.length() > 0)
      {
        // Если текущая строка выходит за пределы экрана, переносим на следующую строку
        if (x + u8g2.getUTF8Width(word.c_str()) > SCREEN_WIDTH)
        {
          numLines++;
          if (numLines == MAX_LINES)
            break;
          x = 0;
          y += u8g2.getMaxCharHeight();
        }
        word = word + " ";
        u8g2.setCursor(x, y);
        u8g2.print(word);
        x += u8g2.getUTF8Width(word.c_str());
        word = "";
      }
      if (c == '\n')
      {
        numLines++;
        if (numLines == MAX_LINES)
          break;
        x = 0;
        y += u8g2.getMaxCharHeight();
      }
    }
    else
    {
      word += c;
    }
  }

  if (word.length() > 0) // если осталось последнее слово
  {
    // Если текущая строка выходит за пределы экрана, переносим на следующую строку
    if (x + u8g2.getUTF8Width(word.c_str()) > SCREEN_WIDTH)
    {
      if (numLines < MAX_LINES)
      {
        x = 0;
        y += u8g2.getMaxCharHeight();
        numLines++;
        u8g2.setCursor(x, y);
        u8g2.print(word);
      }
    }
    else
    {
      u8g2.setCursor(x, y);
      u8g2.print(word);
    }
  }

  //  u8g2.drawUTF8(0, 10, text.c_str());          // write something to the internal memory
  u8g2.sendBuffer(); // transfer internal memory to the display
}

int get_requested_value()
{

  int timeout = 5000;
  int button = 0;
  int amount_ordered = 0;

  send_to_OLED(multilang[5][language]);

  while (1)
  {
    if (xQueueReceive(received_button_Queue, &button, timeout) == pdTRUE)
    {
      if (button >= 0 && button <= 9)
        amount_ordered = amount_ordered * 10 + button;
      if (amount_ordered > 9999)
        amount_ordered = 0;
      SerialDebug.println(amount_ordered, DEC);
      send_to_OLED(String(amount_ordered));

      if (button == 12)
      {
        return amount_ordered;
      }
    }
    else
      return 0;
  }
}

float filling(int amount, String car, String driver)
{

  File file_data;
  File file_quantity;

  int timeout_to_stop = 7000;
  int filled_amount_int = 0;
  float filled_amount_float = 0;
  int amount_filled_saved = 0;
  double counter; // тотал счетчик данного вида топлива
  int filled_pulses = 0;

  long time_amount_saved_to_file = 0;

  int8_t pulser1 = 0;

  StaticJsonDocument<300> doc;

  doc["deviceid"] = chipId;
  doc["date_start"] = rtc.now().unixtime();
  doc["amount_ordered"] = amount;
  doc["user_driver"] = driver;
  doc["car"] = car;
  doc["type_refuel"] = 2;
  doc["type_fuel"] = 1;
  doc["point"] = 1;

  char jsonBuffer[512];

  send_to_OLED(multilang[7][language] + String(amount));

  delay(1500);

  //  xQueueReset(received_pulser_Queue);

  /*включить насос*/

  send_to_OLED(multilang[8][language] + String(filled_amount_float) + multilang[10][language]);

  xSemaphoreTake(mutex_SD_card, portMAX_DELAY); // пишу в резервній файл на случай отключения питания во время заправки
  serializeJson(doc, jsonBuffer);
  file_data = SD.open("/unsaved_data", FILE_WRITE);
  if (!file_data)
    SerialDebug.println("ERROR file unsaved_data could not be created");
  if (!file_data.print(jsonBuffer))
    SerialDebug.println("ERROR could not write to file unsaved_data");
  file_data.close();
  file_quantity = SD.open("/unsaved_quant", FILE_WRITE); // создаю файл с количеством для записи в него в процессе заправки
  file_quantity.close();
  xSemaphoreGive(mutex_SD_card);

  while (1)
  {
    if (xQueueReceive(received_pulser_Queue, &pulser1, timeout_to_stop) == pdTRUE)
    {
      filled_pulses++;
      filled_amount_float = filled_pulses * coeff;
      send_to_OLED(multilang[8][language] + String(filled_amount_float, 2) + multilang[10][language]);
      if (filled_amount_float >= amount)
      {
        /*віключить насос*/
        break;
      }
      if (millis() - time_amount_saved_to_file > 5000)
      {
        xSemaphoreTake(mutex_SD_card, 100); // пишу количество заправленного топлива в резервній файл на случай отключения питания во время заправки
        file_quantity = SD.open("/unsaved_quant", FILE_APPEND);
        if (!file_quantity)
          SerialDebug.println("ERROR opening file unsaved_quant");
        if (!file_quantity.println(String(filled_amount_float, 2)))
          SerialDebug.println("ERROR could not append to file unsaved_quant");
        file_quantity.close();
        time_amount_saved_to_file = millis();
        xSemaphoreGive(mutex_SD_card);
      }
    }
    else
      break;
  }

  counter = counter + filled_amount_float;
  send_to_OLED(multilang[9][language] + String(filled_amount_float, 2) + multilang[10][language]);
  doc["date_end"] = rtc.now().unixtime();
  doc["amount"] = String(filled_amount_float, 2);
  doc["counter"] = String(counter, 2);

  serializeJson(doc, jsonBuffer);
  blackbox_write(jsonBuffer);
  SD.remove("/unsaved_data");
  SD.remove("/unsaved_quant");
  save_config_params_to_flash();

  return filled_amount_float;
}

/* Задачи (tasks)*/

void TaskGetInternet(void *pvParameters)
{

  long wifi_next_try_time = 0;

  //  uint8_t aws_internet3 = 0; // локальная копия глобальной переменной global_aws_internet
  uint8_t internet3 = 0; // локальная копия глобальной переменной global_internet

  xSemaphoreTake(mutex_WiFi, portMAX_DELAY);
  WiFi.onEvent(WiFiEvent);
  xSemaphoreGive(mutex_WiFi);

  SerialDebug.print("TaskGetInternet: Executing on core ");
  SerialDebug.println(xPortGetCoreID());
  SerialDebug.print("TaskGetInternet: priority = ");
  SerialDebug.println(uxTaskPriorityGet(NULL));

  for (;;)
  {

    xSemaphoreTake(mutex_global_variables, portMAX_DELAY);
    //   aws_internet3 = global_aws_internet;
    //   internet3 = global_internet;
    xSemaphoreGive(mutex_global_variables);

    /* добываю интернет */
    switch (internet3)
    {
    case 0: // нет интернета - подключаю WiFi или GSM
            //    aws_internet3 = 0;
      if (config_WiFi && millis() > wifi_next_try_time)
      {
        SerialDebug.println("trying to connect WiFi");
        xSemaphoreTake(mutex_WiFi, portMAX_DELAY);
        if (WiFi_connect())
        {
          xSemaphoreGive(mutex_WiFi);
          vTaskResume(TaskTimeFromNTP_handle);
          delay(1000);
          vTaskResume(TaskWifiAWS_handle);
          internet3 = 2;
          SerialDebug.println("wifi_internet connected");
          //          send_to_OLED("WiFi connected");
          break;
        }
        else
        {
          vTaskSuspend(TaskWifiAWS_handle);
          vTaskSuspend(TaskTimeFromNTP_handle);
          wifi_next_try_time = millis() + 5000; // повторяю через 5 секунд
          SerialDebug.println("wifi_internet not connected");
        }
        xSemaphoreGive(mutex_WiFi);
      }
      if (config_GSM)
      {
        SerialDebug.println("trying to connect GSM");
        xSemaphoreTake(mutex_GSM, portMAX_DELAY);
        if (sim800_connect())
        {
          SerialDebug.println("gsm_internet connected");
          internet3 = 1;
          vTaskResume(TaskGSMAWS_handle);
        }
        else
        {
          SerialDebug.println("gsm_internet not connected");
          vTaskSuspend(TaskGSMAWS_handle);
        }
        xSemaphoreGive(mutex_GSM);
      }
      break;

    case 1: // есть интернет по GSM - пробую подключить WiFi
      if (config_WiFi && millis() > wifi_next_try_time)
      {
        SerialDebug.println("trying to connect WiFi");
        xSemaphoreTake(mutex_WiFi, portMAX_DELAY);
        if (WiFi_connect())
        {
          xSemaphoreGive(mutex_WiFi);
          //          delay(1000);
          internet3 = 2;
          SerialDebug.println("wifi_internet connected");
          break;
        }
        else
        {
          wifi_next_try_time = millis() + 20000; // повторяю через 20 секунд
          SerialDebug.println("wifi_internet not connected");
        }
        xSemaphoreGive(mutex_WiFi);
      }
      break;

    default: // есть WiFi - все ОК, ничего не делаем
      break;
    }

    xSemaphoreTake(mutex_global_variables, portMAX_DELAY);
    //    global_aws_internet = aws_internet3;
    //   global_internet = internet3;
    xSemaphoreGive(mutex_global_variables);
    /* добываю интернет */

    delay(2000);
  }
  vTaskDelete(NULL);
}

void TaskWifiAWS(void *pvParameters)
{

  SSLClient wifisequreclient(wifi_client, TAs, (size_t)TAs_NUM, A5, 1, SSLClient::SSL_ERROR);

  vTaskSuspend(NULL);

  long next_cards_update_time = 0;
  int cards_update_timeout = 80000;   // если обновление карт происходит дольше - ошибка
  long cards_update_request_time = 0; // когда начал обновлять карты
  int cards_update_period = 120000;   // 2 minutes

  uint8_t cards_downloading2 = 0; // локальная копия глобальной переменной global_cards_downloading

  uint8_t aws_internet3 = 0; // локальная копия глобальной переменной global_aws_internet
                             //  uint8_t internet3 = 0;     // локальная копия глобальной переменной global_internet

  long next_send_time = 0;

  SerialDebug.print("TaskWifiAWS: Executing on core ");
  SerialDebug.println(xPortGetCoreID());
  SerialDebug.print("TaskWifiAWS: priority = ");
  SerialDebug.println(uxTaskPriorityGet(NULL));

  for (;;)
  {
    delay(5);
    while (!wifi_mqtt_client.connected())
    {
      connectAWS(wifisequreclient, wifi_mqtt_client);
      delay(100);
    }

    wifi_mqtt_client.loop();

    delay(5);

    /* обрабатываю обновление карточек */
    cards_downloading2 = global_cards_downloading;
    if (cards_downloading2 == 0 && millis() > next_cards_update_time) // шлю сообщение старта
    {
      SerialDebug.println("requesting 1 request");
      if (wifi_mqtt_client.publish("gasstation/card", "{\"deviceid\":\"" + String(chipId) + "\",\"limit\":20,\"skip\":0}", 0, 1))
      {
        cards_downloading2 = 1;
        cards_update_request_time = millis();
        SerialDebug.println("requesting 1 request OK");
      }
      else
      {
        SerialDebug.println("requesting 1 request ERROR");
        wifi_mqtt_client.disconnect();
        next_cards_update_time = millis() + 10000;
        //     delay(5);
        //     continue;
      }
    }

    else if (cards_downloading2 == 1 && millis() > (cards_update_request_time + cards_update_timeout))
    {
      cards_downloading2 = 3; // жду таймаут, если прошло много времени то ошибка
    }
    else if (cards_downloading2 == 2) // карты успешно обновлены
    {
      cards_updated_time = rtc.now().unixtime();
      next_cards_update_time = millis() + cards_update_period;
      cards_downloading2 = 0;
    }
    else if (cards_downloading2 == 3) // при ошибке следующая попытка через 20 сек
    {
      cards_downloading2 = 0;
      next_cards_update_time = millis() + 20000;
    }
    global_cards_downloading = cards_downloading2;
    /* обрабатываю обновление карточек */

    delay(5);

    // vTaskSuspend(TaskSaveLog_handle);
    /*обрабатываю отправку с блекбокса*/
    if (millis() > next_send_time)
    {

      uint8_t send_result = sendfromblackbox_via_mqtt(wifi_mqtt_client);

      switch (send_result)
      {
      case 0:
        SerialDebug.println("Send from blackbox error");
        next_send_time = millis() + 2000;
        wifi_mqtt_client.disconnect();
        //    delay(5);
        //    continue;
        break;

      case 1:
        SerialDebug.println("Send from blackbox OK");
        next_send_time = millis() + 200;
        break;

      case 2:
        //        SerialDebug.println("Empty blackbox");
        next_send_time = millis() + 10000;
        break;

      default:
        break;
      }
    }
    /*обрабатываю отправку с блекбокса*/
    //    vTaskResume(TaskSaveLog_handle);

    delay(5);
  }
  vTaskDelete(NULL);
}

void TaskGSMAWS(void *pvParameters)
{

  SSLClient gsmsequreclient(client, TAs, (size_t)TAs_NUM, A5, 1, SSLClient::SSL_ERROR);

  vTaskSuspend(NULL);

  long next_cards_update_time = 0;
  int cards_update_timeout = 80000;   // если обновление карт происходит дольше - ошибка
  long cards_update_request_time = 0; // когда начал обновлять карты
  int cards_update_period = 120000;   // 2 minutes

  uint8_t cards_downloading2 = 0; // локальная копия глобальной переменной global_cards_downloading

  uint8_t aws_internet3 = 0; // локальная копия глобальной переменной global_aws_internet
                             //  uint8_t internet3 = 0;     // локальная копия глобальной переменной global_internet

  long next_send_time = 0;

  SerialDebug.print("TaskGSMAWS: Executing on core ");
  SerialDebug.println(xPortGetCoreID());
  SerialDebug.print("TaskGSMAWS: priority = ");
  SerialDebug.println(uxTaskPriorityGet(NULL));

  for (;;)
  {
    delay(5);
    while (!gsm_mqtt_client.connected())
    {
      connectAWS(gsmsequreclient, gsm_mqtt_client);
      delay(100);
    }

    gsm_mqtt_client.loop();

    delay(5);

    /* обрабатываю обновление карточек */
    cards_downloading2 = global_cards_downloading;
    if (cards_downloading2 == 0 && millis() > next_cards_update_time) // шлю сообщение старта
    {
      SerialDebug.println("requesting 1 request");
      if (gsm_mqtt_client.publish("gasstation/card", "{\"deviceid\":\"" + String(chipId) + "\",\"limit\":20,\"skip\":0}", 0, 1))
      {
        cards_downloading2 = 1;
        cards_update_request_time = millis();
        SerialDebug.println("requesting 1 request OK");
      }
      else
      {
        SerialDebug.println("requesting 1 request ERROR");
        gsm_mqtt_client.disconnect();
        next_cards_update_time = millis() + 10000;
        //     delay(5);
        //     continue;
      }
    }

    else if (cards_downloading2 == 1 && millis() > (cards_update_request_time + cards_update_timeout))
    {
      cards_downloading2 = 3; // жду таймаут, если прошло много времени то ошибка
    }
    else if (cards_downloading2 == 2) // карты успешно обновлены
    {
      cards_updated_time = rtc.now().unixtime();
      next_cards_update_time = millis() + cards_update_period;
      cards_downloading2 = 0;
    }
    else if (cards_downloading2 == 3) // при ошибке следующая попытка через 20 сек
    {
      cards_downloading2 = 0;
      next_cards_update_time = millis() + 20000;
    }
    global_cards_downloading = cards_downloading2;
    /* обрабатываю обновление карточек */

    delay(5);

    // vTaskSuspend(TaskSaveLog_handle);
    /*обрабатываю отправку с блекбокса*/
    if (millis() > next_send_time)
    {

      uint8_t send_result = sendfromblackbox_via_mqtt(gsm_mqtt_client);

      switch (send_result)
      {
      case 0:
        SerialDebug.println("Send from blackbox error");
        next_send_time = millis() + 2000;
        gsm_mqtt_client.disconnect();
        //    delay(5);
        //    continue;
        break;

      case 1:
        SerialDebug.println("Send from blackbox OK");
        next_send_time = millis() + 200;
        break;

      case 2:
        //        SerialDebug.println("Empty blackbox");
        next_send_time = millis() + 10000;
        break;

      default:
        break;
      }
    }
    /*обрабатываю отправку с блекбокса*/
    //    vTaskResume(TaskSaveLog_handle);

    delay(5);
  }
  vTaskDelete(NULL);
}

void TaskSaveLog(void *pvParameters)
{

  long next_log_save_time = 0;
  int log_save_period = 60000; // 1 minute

  StaticJsonDocument<1024> doc; // for logs
  char jsonBuffer[1024];

  for (;;)
  {
    /* обрабатываю сохранение лога */
    if (millis() > next_log_save_time)
    {
      next_log_save_time = millis() + log_save_period;

      doc["msg_type"] = "log";
      doc["deviceid"] = chipId;
      doc["date"] = rtc.now().unixtime();
      doc["start date"] = start_time;
      doc["last cards update date"] = cards_updated_time;

      doc["WiFi connected"] = WiFi.isConnected();
      doc["WiFi SSID"] = WiFi.SSID();
      doc["WiFi RSSI"] = WiFi.RSSI();

      doc["GSM imei"] = modem.getIMEI();
      doc["GSM signal quality"] = modem.getSignalQuality();
      doc["GSM sim status"] = modem.getSimStatus();
      doc["GSM registration status"] = modem.getRegistrationStatus();
      doc["GSM sim CCID"] = modem.getSimCCID();
      doc["GSM operator"] = modem.getOperator();
      doc["GSM modem"] = modem.getModemName();
      doc["GSM IP"] = modem.getLocalIP();

      serializeJson(doc, jsonBuffer); // print to client
      SerialDebug.print("trying to write to blackbox:    ");
      SerialDebug.println(jsonBuffer);
      if (blackbox_write(jsonBuffer))
        SerialDebug.println("write to blackbox OK");
      else
        SerialDebug.println("write to blackbox ERROR");
    }
    /* обрабатываю сохранение лога */
    delay(100);
  }
  vTaskDelete(NULL);
}

void TaskTimeFromNTP(void *pvParameters)
{

  vTaskSuspend(NULL);

  long next_ntp_update_time = 0;
  int ntp_update_period = 600000; // 10 minutes

  SerialDebug.println("Task TaskTimeFromNTP started");

  SerialDebug.print("TaskTimeFromNTP: Executing on core ");
  SerialDebug.println(xPortGetCoreID());
  SerialDebug.print("TaskTimeFromNTP: priority = ");
  SerialDebug.println(uxTaskPriorityGet(NULL));

  int internet2 = 2; // временно

  for (;;)
  {
    delay(100);

    /* обрабатываю обновление времени по NTP */
    if (millis() > next_ntp_update_time)
    {
      vTaskSuspend(TaskWifiAWS_handle);
      if (internet2 == 1)
      {
        SerialDebug.println("trying to sync time from NTP by GSM");
        if (time_from_ntp("pool.ntp.org", 1))
        {
          SerialDebug.println("GSM time from ntp to rtc updated OK");
          next_ntp_update_time = millis() + ntp_update_period;
        }
        else
        {
          SerialDebug.println("GSM time from ntp to rtc update ERROR");
          next_ntp_update_time = millis();
        }
      }
      else if (internet2 == 2)
      {
        SerialDebug.println("trying to sync time from NTP by WIFI");
        if (time_from_ntp("pool.ntp.org", 2))
        {
          SerialDebug.println("WIFI time from ntp to rtc updated OK");
          next_ntp_update_time = millis() + ntp_update_period;
        }
        else
        {
          SerialDebug.println("WIFI time from ntp to rtc update ERROR");
          next_ntp_update_time = millis();
        }
      }
      vTaskResume(TaskWifiAWS_handle);
    }
    /* обрабатываю обновление времени по NTP */
  }
  vTaskDelete(NULL);
}

void TaskReadSerials(void *pvParameters)
{

  String request = "";
  int button = 0;
  char buffer128[128];
  char buffer64[64];
  char buffer32[32];
  int8_t pulser_1 = 1;

  for (;;)
  {

    delay(5);
    if (SerialSEC.available())
    {
      request = SerialSEC.readStringUntil('\n');
      //      SerialDebug.println(request);

      if (request.substring(0, 4) == "puls")
      {
        xQueueSend(received_pulser_Queue, &pulser_1, 0);
      }
      else if (request.substring(0, 4) == "nfc ")
      {
        request.substring(4).toCharArray(buffer32, 32);
        xQueueSend(received_nfc_Queue, &buffer32, 1000);

        //        SerialDebug.println(buffer32);
      }
      else if (request.substring(0, 7) == "button ")
      {
        button = request.substring(7).toInt();
        xQueueSend(received_button_Queue, &button, 1000);
      }
    }
  }
  vTaskDelete(NULL);
}

void Taskfilling_routine(void *pvParameters)
{

  int button = 0;
  char buffer128[128];
  char buffer64[64];
  char nfc[32];

  bool driver_authorized = false;
  bool car_authorized = false;
  String driver = "";
  String car = "";

  int amount_ordered = 0;
  uint8_t fuel_type = 1;

  long time_unauthorize = 1;
  long time_clear_OLED = 0;
  long time_clear_LED = 0;

  int timeout_unauthorize = 10000;

  card_struct find_in_cards_response;

  for (;;)
  {
    if (time_unauthorize > 0 && millis() > time_unauthorize)
    {
      time_unauthorize = 0;
      driver_authorized = false;
      car_authorized = false;
      driver = "";
      car = "";
      send_to_OLED(multilang[0][language]);
    }

    if (xQueueReceive(received_button_Queue, &button, 0) == pdTRUE)
    {
      send_to_OLED(multilang[1][language]);
      delay(1000);
      send_to_OLED(multilang[0][language]);
    }

    if (xQueueReceive(received_nfc_Queue, &nfc, 10) == pdTRUE)
    {

      time_unauthorize = millis() + timeout_unauthorize;

      //     send_to_OLED("---------NFC---------");

      SerialDebug.printf("received nfc: %s\n", nfc);

      find_in_cards_response = find_in_cards(nfc);

      blackbox_write("{\"msg_type\":\"nfc\",\"date\":" + String(rtc.now().unixtime()) + ",\"nfc\":\"" + nfc + "\"}");

      if (!driver_authorized && find_in_cards_response.is_present && find_in_cards_response.type_card == 3)
      {
        driver = nfc;
        send_to_OLED(find_in_cards_response.user_driver);
        delay(1000);
        driver_authorized = true;
        if (!car_authorized)
          send_to_OLED(multilang[3][language]);
      }
      else if (!car_authorized && find_in_cards_response.is_present && find_in_cards_response.type_card == 4)
      {
        car = nfc;
        send_to_OLED(find_in_cards_response.car);
        delay(1000);
        car_authorized = true;
        if (!driver_authorized)
          send_to_OLED(multilang[4][language]);
      }
      else if (find_in_cards_response.is_present && find_in_cards_response.type_card == 5)
      {
        driver = nfc;
        car = nfc;
        send_to_OLED(find_in_cards_response.car + find_in_cards_response.user_driver);
        delay(1000);
        car_authorized = true;
        driver_authorized = true;
      }
      else
      {
        send_to_OLED(multilang[2][language]);
        time_unauthorize = millis() + 1000;
      }
    }

    if (car_authorized && driver_authorized)
    {
      int requested_value = get_requested_value();
      if (requested_value < 2)
      {
        send_to_OLED(multilang[6][language]);
        delay(1000);
        requested_value = get_requested_value();
      }

      if (requested_value < 2)
      {
        car_authorized = false;
        driver_authorized = false;
        time_unauthorize = millis();
      }
      else
      {
        float filled = filling(requested_value, car, driver);
        if (filled > 0)
        {
          SerialDebug.println("A");
          save_total_counter(fuel_type);
          SerialDebug.println("B");
          car_authorized = false;
          SerialDebug.println("C");
          driver_authorized = false;
          SerialDebug.println("D");
          time_unauthorize = millis() + 8000;
          SerialDebug.println("E");
        }
      }
    }
  }
  vTaskDelete(NULL);
}