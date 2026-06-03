import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  vus: 5,          // 5 tane eşzamanlı sanal iOS cihazı
  duration: '10s', // Test 10 saniye boyunca sürecek
};

export default function () {
  // Postman'deki doğru URL
  const url = 'http://localhost:8080/api/v1/locations/ingest'; 

  // İstanbul koordinatları etrafında küçük rastgele sapmalar üreterek cihazları hareket ettiriyoruz
  const payload = JSON.stringify({
    device_id: `device_sim_${__VU}`, // device_sim_1, device_sim_2 şeklinde benzersiz id'ler
    latitude: 41.0082 + (Math.random() - 0.5) * 0.02,
    longitude: 28.9784 + (Math.random() - 0.5) * 0.02,
    timestamp: Date.now() // Go tarafının beklediği 13 haneli Unix Milliseconds tipi
  });

  const params = {
    headers: { 'Content-Type': 'application/json' },
  };

  // İsteği gönder
  http.post(url, payload, params);

  // Her sanal cihaz yarım saniyede bir yeni konum göndersin
  sleep(0.5); 
}