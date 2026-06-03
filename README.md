# Distributed Real-Time Geofencing & State Engine

Bu proje; binlerce mobil cihazdan veya kuryeden gelen anlık konum verilerini (GPS) asenkron olarak işleyen, coğrafi sınırları (Geofence) milisaniyeler içinde tespit eden ve cihazların bölgeye giriş/çıkış anlarını **büyük veri (Big Data) ve dağıtık sistem mimarileri** kullanarak yöneten yüksek performanslı bir backend motorudur.

Projenin temel amacı; Uber, Getir veya BiTaksi gibi canlı operasyon yöneten sistemlerdeki **"kurye mahalleye girdi"** veya **"sürücü dinamik fiyatlandırma bölgesinden çıktı"** gibi anlık bildirimleri, veritabanını yormadan ve kullanıcılara mükerrer (spam) bildirimler göndermeden **State Machine (Durum Makinesi)** mantığıyla çözmektir.

---

## Mimaride "Neyi, Neden" Yaptık? (Teknik Kararlar)

Kod tabanını incelerken mimari katmanların seçimindeki mühendislik nedenleri aşağıda açıklanmıştır:

### 1. Dil Seçimi: Neden Go (Golang)?
* **Neden?:** Projenin çekirdeğini oluşturan Kafka Consumer katmanında binlerce eşzamanlı (concurrent) konumu en düşük RAM ve CPU maliyetiyle işlemek gerekiyordu. 
* **Çözüm:** Go'nun **Goroutine** ve **Channel** mekanizmaları sayesinde sıfıra yakın kilitlenme (deadlock) riskiyle, minimal kaynak harcayarak saniyede binlerce veriyi asenkron işleyebilen bir Consumer tasarladık.

### 2. Veri Kuyruğu: Neden Apache Kafka?
* **Neden?:** Mobil cihazlar aynı anda binlerce `POST /ingest` isteği attığında, doğrudan veritabanına yüklenmek sistemi çökertecektir (Spike/Traffic Surge).
* **Çözüm:** Konum servisimiz (`LocationService`) gelen ham GPS verilerini hemen veritabanına yazmaz. Sadece geçerli bir JSON olduğunu doğrular ve **Apache Kafka** üzerindeki `location-updates` topic'ine fırlatır. Bu sayede HTTP API katmanımız `202 Accepted` dönerek anında boşa çıkar ve sistem darboğaz yaşamaz.

### 3. Coğrafi Zeka: Neden PostgreSQL + PostGIS?
* **Neden?:** Standart veritabanları enlem ve boylamı sadece iki `float` sayı olarak saklar. Bir noktanın karmaşık bir poligonun (Örn: Sultanahmet Meydanı) içinde olup olmadığını standart matematiksel sorgularla (SQL `WHERE` clauses) bulmak performans felaketidir.
* **Çözüm:** Sektör standardı olan **PostGIS** uzantısını entegre ettik. Poligonları `GEOMETRY` tipinde tutup üzerlerine **GIST (Spatial Index)** tanımladık. Uygulama `ST_Contains` fonksiyonuyla `ST_MakePoint(lng, lat)` sorgusu attığında mikro saniyeler (`~1.1ms`) seviyesinde poligon eşleşmesi tamamlanır.

### 4. Durum Yönetimi ve Spam Filtresi: Neden Redis State Machine?
* **Sorun (Kritik):** Bir kurye poligonun içinde gezinirken sürekli GPS verisi gönderir. Eğer her `rows:1` (içeride) sonucunda kullanıcıya "Alana girdi" bildirimi atarsak, kullanıcının telefonu saniyede 10 kere push notification ile kilitlenir. Ayrıca PostGIS'e her saniye gereksiz SELECT sorguları yüklenir.
* **Çözüm:** PostGIS katmanının önüne **Redis tabanlı bir Dağıtık Durum Makinesi (Distributed State Machine)** konumlandırdık.
  * Cihaz alana **ilk kez** girdiğinde (`PostGIS = INSIDE && Redis = NONE`), bu bir **ENTER** olayıdır. Redis'e `geofence:device_id -> zone_name` anahtarı yazılır ve alarm fırlatılır.
  * Cihaz içeride gezinirken (`PostGIS = INSIDE && Redis = YES`), Redis cihazın zaten içeride olduğunu bilir. **Mükerrer log ve alarm üretilmesi sessizce engellenir (Spam Suppression).**
  * Cihaz alandan çıktığında (`PostGIS = OUTSIDE && Redis = YES`), bu bir **EXIT** olayıdır. Redis'teki anahtar silinir ve çıkış alarmı fırlatılır.

---

## Durum Makinesi (State Machine) Mantıksal Akışı

Sistemin kararları nasıl verdiğini gösteren log hikayesi aşağıdaki gibidir:

1. **Zaten Dışarıda Durumu (Outside -> Outside):**
   * PostGIS: Sınırlar içinde DEĞİL (`rows:0`).
   * Redis: Hafızada kayıt YOK.
   * **Eylem:** Sessizce geçilir.

2. **İlk Giriş Anı (Outside -> Inside - ENTER):**
   * PostGIS: Sınırlar İÇİNDE (`rows:1`) -> *Sultanahmet Meydanı*
   * Redis: Hafızada kayıt YOK (`HAYIR - İlk defa giriyor`).
   * **Eylem:** `geofence:test_kurye_1` anahtarı Redis'e yazılır ve `[Geofence ENTER]` tetiklenir.

3. **İçeride Gezinme (Inside -> Inside - SPAM FİLTRESİ):**
   * PostGIS: Sınırlar İÇİNDE (`rows:1`) -> *Sultanahmet Meydanı*
   * Redis: Hafızada kayıt VAR (`EVET - Zaten içeride`).
   * **Eylem:** Bildirim ve log üretimi engellenir. Sessizce geçildi.

4. **Çıkış Anı (Inside -> Outside - EXIT):**
   * PostGIS: Sınırlar içinde DEĞİL (`rows:0`).
   * Redis: Hafızada kayıt VAR (`EVET - En son içerideydi`).
   * **Eylem:** Redis'teki anahtar silinir ve `[Geofence EXIT]` tetiklenir.

---

## Proje Yapısı (Directory Tree)

```text
.

├── Dockerfile
├── README.md
├── docker-compose.yml
├── go.mod
├── go.sum
├── load_test.js
├── cmd
│   └── server
│       └── main.go
├── internal
│   ├── api
│   │   ├── handler
│   │   │   └── handler.go
│   │   ├── model
│   │   │   ├── geofence.go
│   │   │   └── location.go
│   │   ├── repository
│   │   │   ├── geofence_repository.go
│   │   │   └── location_repository.go
│   │   ├── service
│   │   │   └── location_service.go
│   │   └── route.go
│   ├── db
│   │   ├── db.go
│   │   └── migrations
│   │       ├── 000001_create_locations.down.sql
│   │       ├── 000001_create_locations.up.sql
│   │       ├── 000002_create_geofences.down.sql
│   │       └── 000002_create_geofences.up.sql
│   └── transport
│       └── kafka
│           ├── consumer.go
│           └── producer.go
└── pkg
   ├── postgres
   │   └── postgres.go
   └── redis
      └── redis.go
```
